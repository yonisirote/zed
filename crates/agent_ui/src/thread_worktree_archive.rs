use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::Arc,
};

use agent_client_protocol as acp;
use anyhow::{Context as _, Result, anyhow};
use collections::HashMap;
use git::repository::{AskPassDelegate, CommitOptions, DEFAULT_WORKTREE_DIRECTORY, ResetMode};
use gpui::{App, AsyncApp, Entity, Global, Task, WindowHandle};
use parking_lot::Mutex;
use project::{
    LocalProjectFlags, Project, WorktreeId, git_store::Repository, worktrees_directory_for_repo,
};
use util::ResultExt;
use workspace::{
    AppState, MultiWorkspace, OpenMode, OpenOptions, PathList, Toast, Workspace,
    notifications::NotificationId, open_new, open_paths,
};

use crate::thread_metadata_store::{ArchivedGitWorktree, ThreadMetadataStore};

#[derive(Default)]
pub struct ThreadArchiveCleanupCoordinator {
    in_flight_roots: Mutex<HashSet<PathBuf>>,
}

impl Global for ThreadArchiveCleanupCoordinator {}

fn ensure_global(cx: &mut App) {
    if !cx.has_global::<ThreadArchiveCleanupCoordinator>() {
        cx.set_global(ThreadArchiveCleanupCoordinator::default());
    }
}

#[derive(Clone)]
pub struct ArchiveOutcome {
    pub archived_immediately: bool,
    pub roots_to_delete: Vec<PathBuf>,
}

#[derive(Clone)]
struct RootPlan {
    root_path: PathBuf,
    main_repo_path: PathBuf,
    affected_projects: Vec<AffectedProject>,
    worktree_repo: Option<Entity<Repository>>,
    branch_name: Option<String>,
}

#[derive(Clone)]
struct AffectedProject {
    project: Entity<Project>,
    worktree_id: WorktreeId,
}

#[derive(Clone)]
enum FallbackTarget {
    ExistingWorkspace {
        window: WindowHandle<MultiWorkspace>,
        workspace: Entity<Workspace>,
    },
    OpenPaths {
        requesting_window: WindowHandle<MultiWorkspace>,
        paths: Vec<PathBuf>,
    },
    OpenEmpty {
        requesting_window: WindowHandle<MultiWorkspace>,
    },
}

#[derive(Clone)]
struct CleanupPlan {
    folder_paths: PathList,
    roots: Vec<RootPlan>,
    current_workspace: Option<Entity<Workspace>>,
    current_workspace_will_be_empty: bool,
    fallback: Option<FallbackTarget>,
    affected_workspaces: Vec<Entity<Workspace>>,
}

fn archived_worktree_ref_name(id: i64) -> String {
    format!("refs/archived-worktrees/{}", id)
}

struct PersistOutcome {
    archived_worktree_id: i64,
    staged_commit_hash: String,
}

pub fn archive_thread(
    session_id: &acp::SessionId,
    current_workspace: Option<Entity<Workspace>>,
    window: WindowHandle<MultiWorkspace>,
    cx: &mut App,
) -> ArchiveOutcome {
    ensure_global(cx);
    let plan = build_cleanup_plan(session_id, current_workspace, window, cx);

    ThreadMetadataStore::global(cx).update(cx, |store, cx| store.archive(session_id, cx));

    if let Some(plan) = plan {
        let roots_to_delete = plan
            .roots
            .iter()
            .map(|root| root.root_path.clone())
            .collect::<Vec<_>>();
        if !roots_to_delete.is_empty() {
            cx.spawn(async move |cx| {
                run_cleanup(plan, cx).await;
            })
            .detach();

            return ArchiveOutcome {
                archived_immediately: true,
                roots_to_delete,
            };
        }
    }

    ArchiveOutcome {
        archived_immediately: true,
        roots_to_delete: Vec::new(),
    }
}

fn build_cleanup_plan(
    session_id: &acp::SessionId,
    current_workspace: Option<Entity<Workspace>>,
    requesting_window: WindowHandle<MultiWorkspace>,
    cx: &App,
) -> Option<CleanupPlan> {
    let metadata = ThreadMetadataStore::global(cx)
        .read(cx)
        .entry(session_id)
        .cloned()?;

    let workspaces = all_open_workspaces(cx);

    let candidate_roots = metadata
        .folder_paths
        .ordered_paths()
        .filter_map(|path| build_root_plan(path, &workspaces, cx))
        .filter(|plan| {
            !path_is_referenced_by_other_unarchived_threads(session_id, &plan.root_path, cx)
        })
        .collect::<Vec<_>>();

    if candidate_roots.is_empty() {
        return Some(CleanupPlan {
            folder_paths: metadata.folder_paths,
            roots: Vec::new(),
            current_workspace,
            current_workspace_will_be_empty: false,
            fallback: None,
            affected_workspaces: Vec::new(),
        });
    }

    let mut affected_workspaces = Vec::new();
    let mut current_workspace_will_be_empty = false;

    for workspace in workspaces.iter() {
        let doomed_root_count = workspace
            .read(cx)
            .root_paths(cx)
            .into_iter()
            .filter(|path| {
                candidate_roots
                    .iter()
                    .any(|root| root.root_path.as_path() == path.as_ref())
            })
            .count();

        if doomed_root_count == 0 {
            continue;
        }

        let surviving_root_count = workspace
            .read(cx)
            .root_paths(cx)
            .len()
            .saturating_sub(doomed_root_count);
        if current_workspace
            .as_ref()
            .is_some_and(|current| current == workspace)
        {
            current_workspace_will_be_empty = surviving_root_count == 0;
        }
        affected_workspaces.push(workspace.clone());
    }

    let fallback = if current_workspace_will_be_empty {
        choose_fallback_target(
            session_id,
            current_workspace.as_ref(),
            &candidate_roots,
            &requesting_window,
            &workspaces,
            cx,
        )
    } else {
        None
    };

    Some(CleanupPlan {
        folder_paths: metadata.folder_paths,
        roots: candidate_roots,
        current_workspace,
        current_workspace_will_be_empty,
        fallback,
        affected_workspaces,
    })
}

fn build_root_plan(path: &Path, workspaces: &[Entity<Workspace>], cx: &App) -> Option<RootPlan> {
    let path = path.to_path_buf();
    let affected_projects = workspaces
        .iter()
        .filter_map(|workspace| {
            let project = workspace.read(cx).project().clone();
            let worktree = project
                .read(cx)
                .visible_worktrees(cx)
                .find(|worktree| worktree.read(cx).abs_path().as_ref() == path.as_path())?;
            let worktree_id = worktree.read(cx).id();
            Some(AffectedProject {
                project,
                worktree_id,
            })
        })
        .collect::<Vec<_>>();

    let (linked_snapshot, worktree_repo) = workspaces
        .iter()
        .flat_map(|workspace| {
            workspace
                .read(cx)
                .project()
                .read(cx)
                .repositories(cx)
                .values()
                .cloned()
                .collect::<Vec<_>>()
        })
        .find_map(|repo| {
            let snapshot = repo.read(cx).snapshot();
            (snapshot.is_linked_worktree()
                && snapshot.work_directory_abs_path.as_ref() == path.as_path())
            .then_some((snapshot, repo))
        })?;

    let branch_name = linked_snapshot
        .branch
        .as_ref()
        .map(|b| b.name().to_string());

    Some(RootPlan {
        root_path: path,
        main_repo_path: linked_snapshot.original_repo_abs_path.to_path_buf(),
        affected_projects,
        worktree_repo: Some(worktree_repo),
        branch_name,
    })
}

fn path_is_referenced_by_other_unarchived_threads(
    current_session_id: &acp::SessionId,
    path: &Path,
    cx: &App,
) -> bool {
    ThreadMetadataStore::global(cx)
        .read(cx)
        .entries()
        .filter(|thread| thread.session_id != *current_session_id)
        .filter(|thread| !thread.archived)
        .any(|thread| {
            thread
                .folder_paths
                .paths()
                .iter()
                .any(|other_path| other_path.as_path() == path)
        })
}

fn choose_fallback_target(
    current_session_id: &acp::SessionId,
    current_workspace: Option<&Entity<Workspace>>,
    roots: &[RootPlan],
    requesting_window: &WindowHandle<MultiWorkspace>,
    workspaces: &[Entity<Workspace>],
    cx: &App,
) -> Option<FallbackTarget> {
    let doomed_roots = roots
        .iter()
        .map(|root| root.root_path.clone())
        .collect::<HashSet<_>>();

    let surviving_same_window = requesting_window.read(cx).ok().and_then(|multi_workspace| {
        multi_workspace
            .workspaces()
            .iter()
            .filter(|workspace| current_workspace.is_none_or(|current| *workspace != current))
            .find(|workspace| workspace_survives(workspace, &doomed_roots, cx))
            .cloned()
    });
    if let Some(workspace) = surviving_same_window {
        return Some(FallbackTarget::ExistingWorkspace {
            window: *requesting_window,
            workspace,
        });
    }

    for window in cx
        .windows()
        .into_iter()
        .filter_map(|window| window.downcast::<MultiWorkspace>())
    {
        if window == *requesting_window {
            continue;
        }
        if let Ok(multi_workspace) = window.read(cx) {
            if let Some(workspace) = multi_workspace
                .workspaces()
                .iter()
                .find(|workspace| workspace_survives(workspace, &doomed_roots, cx))
                .cloned()
            {
                return Some(FallbackTarget::ExistingWorkspace { window, workspace });
            }
        }
    }

    let safe_thread_workspace = ThreadMetadataStore::global(cx)
        .read(cx)
        .entries()
        .filter(|metadata| metadata.session_id != *current_session_id && !metadata.archived)
        .filter_map(|metadata| {
            workspaces
                .iter()
                .find(|workspace| workspace_path_list(workspace, cx) == metadata.folder_paths)
                .cloned()
        })
        .find(|workspace| workspace_survives(workspace, &doomed_roots, cx));

    if let Some(workspace) = safe_thread_workspace {
        let window = window_for_workspace(&workspace, cx).unwrap_or(*requesting_window);
        return Some(FallbackTarget::ExistingWorkspace { window, workspace });
    }

    if let Some(root) = roots.first() {
        return Some(FallbackTarget::OpenPaths {
            requesting_window: *requesting_window,
            paths: vec![root.main_repo_path.clone()],
        });
    }

    Some(FallbackTarget::OpenEmpty {
        requesting_window: *requesting_window,
    })
}

async fn run_cleanup(plan: CleanupPlan, cx: &mut AsyncApp) {
    let roots_to_delete =
        cx.update_global::<ThreadArchiveCleanupCoordinator, _>(|coordinator, _cx| {
            let mut in_flight_roots = coordinator.in_flight_roots.lock();
            plan.roots
                .iter()
                .filter_map(|root| {
                    if in_flight_roots.insert(root.root_path.clone()) {
                        Some(root.clone())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
        });

    if roots_to_delete.is_empty() {
        return;
    }

    let active_workspace = plan.current_workspace.clone();
    if let Some(workspace) = active_workspace
        .as_ref()
        .filter(|_| plan.current_workspace_will_be_empty)
    {
        let Some(window) = window_for_workspace_async(workspace, cx) else {
            release_in_flight_roots(&roots_to_delete, cx);
            return;
        };

        let should_continue = save_workspace_for_root_removal(workspace.clone(), window, cx).await;
        if !should_continue {
            release_in_flight_roots(&roots_to_delete, cx);
            return;
        }
    }

    for workspace in plan
        .affected_workspaces
        .iter()
        .filter(|workspace| Some((*workspace).clone()) != active_workspace)
    {
        let Some(window) = window_for_workspace_async(workspace, cx) else {
            continue;
        };

        if !save_workspace_for_root_removal(workspace.clone(), window, cx).await {
            release_in_flight_roots(&roots_to_delete, cx);
            return;
        }
    }

    if plan.current_workspace_will_be_empty {
        if let Some(fallback) = plan.fallback.clone() {
            activate_fallback(fallback, cx).await.log_err();
        }
    }

    let mut git_removal_errors: Vec<(PathBuf, anyhow::Error)> = Vec::new();
    let mut persist_errors: Vec<(PathBuf, anyhow::Error)> = Vec::new();
    let mut persist_outcomes: HashMap<PathBuf, PersistOutcome> = HashMap::default();

    for root in &roots_to_delete {
        if root.worktree_repo.is_some() {
            match persist_worktree_state(root, &plan, cx).await {
                Ok(outcome) => {
                    persist_outcomes.insert(root.root_path.clone(), outcome);
                }
                Err(error) => {
                    log::error!(
                        "Failed to persist worktree state for {}: {error}",
                        root.root_path.display()
                    );
                    persist_errors.push((root.root_path.clone(), error));
                    continue;
                }
            }
        }

        if let Err(error) = remove_root(root.clone(), cx).await {
            if let Some(outcome) = persist_outcomes.remove(&root.root_path) {
                rollback_persist(&outcome, root, cx).await;
            }
            git_removal_errors.push((root.root_path.clone(), error));
        }
    }

    cleanup_empty_workspaces(&plan.affected_workspaces, cx).await;

    let all_errors: Vec<(PathBuf, anyhow::Error)> = persist_errors
        .into_iter()
        .chain(git_removal_errors)
        .collect();

    if !all_errors.is_empty() {
        let detail = all_errors
            .into_iter()
            .map(|(path, error)| format!("{}: {error}", path.display()))
            .collect::<Vec<_>>()
            .join("\n");
        show_error_toast(
            "Thread archived, but linked worktree cleanup failed",
            &detail,
            &plan,
            cx,
        );
    }

    release_in_flight_roots(&roots_to_delete, cx);
}

async fn save_workspace_for_root_removal(
    workspace: Entity<Workspace>,
    window: WindowHandle<MultiWorkspace>,
    cx: &mut AsyncApp,
) -> bool {
    let has_dirty_items = workspace.read_with(cx, |workspace, cx| {
        workspace.items(cx).any(|item| item.is_dirty(cx))
    });

    if has_dirty_items {
        let _ = window.update(cx, |multi_workspace, window, cx| {
            window.activate_window();
            multi_workspace.activate(workspace.clone(), window, cx);
        });
    }

    let save_task = window.update(cx, |_multi_workspace, window, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.save_for_root_removal(window, cx)
        })
    });

    let Ok(task) = save_task else {
        return false;
    };

    task.await.unwrap_or(false)
}

async fn activate_fallback(target: FallbackTarget, cx: &mut AsyncApp) -> Result<()> {
    match target {
        FallbackTarget::ExistingWorkspace { window, workspace } => {
            window.update(cx, |multi_workspace, window, cx| {
                window.activate_window();
                multi_workspace.activate(workspace, window, cx);
            })?;
        }
        FallbackTarget::OpenPaths {
            requesting_window,
            paths,
        } => {
            let app_state = current_app_state(cx).context("no workspace app state available")?;
            cx.update(|cx| {
                open_paths(
                    &paths,
                    app_state,
                    OpenOptions {
                        requesting_window: Some(requesting_window),
                        open_mode: OpenMode::Activate,
                        ..Default::default()
                    },
                    cx,
                )
            })
            .await?;
        }
        FallbackTarget::OpenEmpty { requesting_window } => {
            let app_state = current_app_state(cx).context("no workspace app state available")?;
            cx.update(|cx| {
                open_new(
                    OpenOptions {
                        requesting_window: Some(requesting_window),
                        open_mode: OpenMode::Activate,
                        ..Default::default()
                    },
                    app_state,
                    cx,
                    |_workspace, _window, _cx| {},
                )
            })
            .await?;
        }
    }

    Ok(())
}

async fn remove_root(root: RootPlan, cx: &mut AsyncApp) -> Result<()> {
    let release_tasks: Vec<_> = root
        .affected_projects
        .iter()
        .map(|affected| {
            let project = affected.project.clone();
            let worktree_id = affected.worktree_id;
            project.update(cx, |project, cx| {
                let wait = project.wait_for_worktree_release(worktree_id, cx);
                project.remove_worktree(worktree_id, cx);
                wait
            })
        })
        .collect();

    if let Err(error) = remove_root_after_worktree_removal(&root, release_tasks, cx).await {
        rollback_root(&root, cx).await;
        return Err(error);
    }

    Ok(())
}

async fn remove_root_after_worktree_removal(
    root: &RootPlan,
    release_tasks: Vec<Task<Result<()>>>,
    cx: &mut AsyncApp,
) -> Result<()> {
    for task in release_tasks {
        task.await?;
    }

    let (repo, _temp_project) = find_or_create_repository(&root.main_repo_path, cx).await?;
    let receiver = repo.update(cx, |repo: &mut Repository, _cx| {
        repo.remove_worktree(root.root_path.clone(), false)
    });
    let result = receiver
        .await
        .map_err(|_| anyhow!("git worktree removal was canceled"))?;
    result
}

/// Finds a live `Repository` entity for the given path, or creates a temporary
/// `Project::local` to obtain one.
///
/// `Repository` entities can only be obtained through a `Project` because
/// `GitStore` (which creates and manages `Repository` entities) is owned by
/// `Project`. When no open workspace contains the repo we need, we spin up a
/// headless `Project::local` just to get a `Repository` handle. The caller
/// keeps the returned `Option<Entity<Project>>` alive for the duration of the
/// git operations, then drops it.
///
/// Future improvement: decoupling `GitStore` from `Project` so that
/// `Repository` entities can be created standalone would eliminate this
/// temporary-project workaround.
async fn find_or_create_repository(
    repo_path: &Path,
    cx: &mut AsyncApp,
) -> Result<(Entity<Repository>, Option<Entity<Project>>)> {
    let repo_path_owned = repo_path.to_path_buf();
    let live_repo = cx.update(|cx| {
        all_open_workspaces(cx)
            .into_iter()
            .flat_map(|workspace| {
                workspace
                    .read(cx)
                    .project()
                    .read(cx)
                    .repositories(cx)
                    .values()
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .find(|repo| {
                repo.read(cx).snapshot().work_directory_abs_path.as_ref()
                    == repo_path_owned.as_path()
            })
    });

    if let Some(repo) = live_repo {
        return Ok((repo, None));
    }

    let app_state =
        current_app_state(cx).context("no app state available for temporary project")?;
    let temp_project = cx.update(|cx| {
        Project::local(
            app_state.client.clone(),
            app_state.node_runtime.clone(),
            app_state.user_store.clone(),
            app_state.languages.clone(),
            app_state.fs.clone(),
            None,
            LocalProjectFlags::default(),
            cx,
        )
    });

    let repo_path_for_worktree = repo_path.to_path_buf();
    let create_worktree = temp_project.update(cx, |project, cx| {
        project.create_worktree(repo_path_for_worktree, true, cx)
    });
    let _worktree = create_worktree.await?;
    let initial_scan = temp_project.read_with(cx, |project, cx| project.wait_for_initial_scan(cx));
    initial_scan.await;

    let repo_path_for_find = repo_path.to_path_buf();
    let repo = temp_project
        .update(cx, |project, cx| {
            project
                .repositories(cx)
                .values()
                .find(|repo| {
                    repo.read(cx).snapshot().work_directory_abs_path.as_ref()
                        == repo_path_for_find.as_path()
                })
                .cloned()
        })
        .context("failed to resolve temporary repository handle")?;

    let barrier = repo.update(cx, |repo: &mut Repository, _cx| repo.barrier());
    barrier
        .await
        .map_err(|_| anyhow!("temporary repository barrier canceled"))?;
    Ok((repo, Some(temp_project)))
}

async fn rollback_root(root: &RootPlan, cx: &mut AsyncApp) {
    for affected in &root.affected_projects {
        let task = affected.project.update(cx, |project, cx| {
            project.create_worktree(root.root_path.clone(), true, cx)
        });
        let _ = task.await;
    }
}

async fn persist_worktree_state(
    root: &RootPlan,
    plan: &CleanupPlan,
    cx: &mut AsyncApp,
) -> Result<PersistOutcome> {
    let worktree_repo = root
        .worktree_repo
        .clone()
        .context("no worktree repo entity for persistence")?;

    // Step 1: Create WIP commit #1 (staged state)
    let askpass = AskPassDelegate::new(cx, |_, _, _| {});
    let commit_rx = worktree_repo.update(cx, |repo, cx| {
        repo.commit(
            "WIP staged".into(),
            None,
            CommitOptions {
                allow_empty: true,
                ..Default::default()
            },
            askpass,
            cx,
        )
    });
    commit_rx
        .await
        .map_err(|_| anyhow!("WIP staged commit canceled"))??;

    // Read SHA after staged commit
    let staged_sha_result = worktree_repo
        .update(cx, |repo, _cx| repo.head_sha())
        .await
        .map_err(|_| anyhow!("head_sha canceled"))
        .and_then(|r| r.context("failed to read HEAD SHA after staged commit"))
        .and_then(|opt| opt.context("HEAD SHA is None after staged commit"));
    let staged_commit_hash = match staged_sha_result {
        Ok(sha) => sha,
        Err(error) => {
            let rx = worktree_repo.update(cx, |repo, cx| {
                repo.reset("HEAD~1".to_string(), ResetMode::Mixed, cx)
            });
            let _ = rx.await;
            return Err(error);
        }
    };

    // Step 2: Stage all files including untracked
    let stage_rx = worktree_repo.update(cx, |repo, _cx| repo.stage_all_including_untracked());
    if let Err(error) = stage_rx
        .await
        .map_err(|_| anyhow!("stage all canceled"))
        .and_then(|inner| inner)
    {
        let rx = worktree_repo.update(cx, |repo, cx| {
            repo.reset("HEAD~1".to_string(), ResetMode::Mixed, cx)
        });
        let _ = rx.await;
        return Err(error.context("failed to stage all files including untracked"));
    }

    // Step 3: Create WIP commit #2 (unstaged/untracked state)
    let askpass = AskPassDelegate::new(cx, |_, _, _| {});
    let commit_rx = worktree_repo.update(cx, |repo, cx| {
        repo.commit(
            "WIP unstaged".into(),
            None,
            CommitOptions {
                allow_empty: true,
                ..Default::default()
            },
            askpass,
            cx,
        )
    });
    if let Err(error) = commit_rx
        .await
        .map_err(|_| anyhow!("WIP unstaged commit canceled"))
        .and_then(|inner| inner)
    {
        let rx = worktree_repo.update(cx, |repo, cx| {
            repo.reset("HEAD~1".to_string(), ResetMode::Mixed, cx)
        });
        let _ = rx.await;
        return Err(error);
    }

    // Step 4: Read HEAD SHA after WIP commits
    let head_sha_result = worktree_repo
        .update(cx, |repo, _cx| repo.head_sha())
        .await
        .map_err(|_| anyhow!("head_sha canceled"))
        .and_then(|r| r.context("failed to read HEAD SHA after WIP commits"))
        .and_then(|opt| opt.context("HEAD SHA is None after WIP commits"));
    let unstaged_commit_hash = match head_sha_result {
        Ok(sha) => sha,
        Err(error) => {
            let rx = worktree_repo.update(cx, |repo, cx| {
                repo.reset(format!("{}~1", staged_commit_hash), ResetMode::Mixed, cx)
            });
            let _ = rx.await;
            return Err(error);
        }
    };

    // Step 5: Create DB record
    let store = cx.update(|cx| ThreadMetadataStore::global(cx));
    let worktree_path_str = root.root_path.to_string_lossy().to_string();
    let main_repo_path_str = root.main_repo_path.to_string_lossy().to_string();
    let branch_name = root.branch_name.clone();

    let db_result = store
        .read_with(cx, |store, cx| {
            store.create_archived_worktree(
                &worktree_path_str,
                &main_repo_path_str,
                branch_name.as_deref(),
                &staged_commit_hash,
                &unstaged_commit_hash,
                cx,
            )
        })
        .await
        .context("failed to create archived worktree DB record");
    let archived_worktree_id = match db_result {
        Ok(id) => id,
        Err(error) => {
            let rx = worktree_repo.update(cx, |repo, cx| {
                repo.reset(format!("{}~1", staged_commit_hash), ResetMode::Mixed, cx)
            });
            let _ = rx.await;
            return Err(error);
        }
    };

    // Step 6: Link all threads on this worktree to the archived record
    let session_ids: Vec<acp::SessionId> = store.read_with(cx, |store, _cx| {
        store
            .all_session_ids_for_path(&plan.folder_paths)
            .cloned()
            .collect()
    });

    for session_id in &session_ids {
        let link_result = store
            .read_with(cx, |store, cx| {
                store.link_thread_to_archived_worktree(&session_id.0, archived_worktree_id, cx)
            })
            .await;
        if let Err(error) = link_result {
            if let Err(delete_error) = store
                .read_with(cx, |store, cx| {
                    store.delete_archived_worktree(archived_worktree_id, cx)
                })
                .await
            {
                log::error!(
                    "Failed to delete archived worktree DB record during link rollback: {delete_error:#}"
                );
            }
            let rx = worktree_repo.update(cx, |repo, cx| {
                repo.reset(format!("{}~1", staged_commit_hash), ResetMode::Mixed, cx)
            });
            let _ = rx.await;
            return Err(error.context("failed to link thread to archived worktree"));
        }
    }

    // Step 7: Create git ref on main repo (non-fatal)
    let ref_name = archived_worktree_ref_name(archived_worktree_id);
    let main_repo_result = find_or_create_repository(&root.main_repo_path, cx).await;
    match main_repo_result {
        Ok((main_repo, _temp_project)) => {
            let rx = main_repo.update(cx, |repo, _cx| {
                repo.update_ref(ref_name.clone(), unstaged_commit_hash.clone())
            });
            if let Err(error) = rx
                .await
                .map_err(|_| anyhow!("update_ref canceled"))
                .and_then(|r| r)
            {
                log::warn!(
                    "Failed to create ref {} on main repo (non-fatal): {error}",
                    ref_name
                );
            }
        }
        Err(error) => {
            log::warn!(
                "Could not find main repo to create ref {} (non-fatal): {error}",
                ref_name
            );
        }
    }

    Ok(PersistOutcome {
        archived_worktree_id,
        staged_commit_hash,
    })
}

async fn rollback_persist(outcome: &PersistOutcome, root: &RootPlan, cx: &mut AsyncApp) {
    // Undo WIP commits on the worktree repo
    if let Some(worktree_repo) = &root.worktree_repo {
        let rx = worktree_repo.update(cx, |repo, cx| {
            repo.reset(
                format!("{}~1", outcome.staged_commit_hash),
                ResetMode::Mixed,
                cx,
            )
        });
        let _ = rx.await;
    }

    // Delete the git ref on main repo
    if let Ok((main_repo, _temp_project)) =
        find_or_create_repository(&root.main_repo_path, cx).await
    {
        let ref_name = archived_worktree_ref_name(outcome.archived_worktree_id);
        let rx = main_repo.update(cx, |repo, _cx| repo.delete_ref(ref_name));
        let _ = rx.await;
    }

    // Delete the DB record
    let store = cx.update(|cx| ThreadMetadataStore::global(cx));
    if let Err(error) = store
        .read_with(cx, |store, cx| {
            store.delete_archived_worktree(outcome.archived_worktree_id, cx)
        })
        .await
    {
        log::error!("Failed to delete archived worktree DB record during rollback: {error:#}");
    }
}

async fn cleanup_empty_workspaces(workspaces: &[Entity<Workspace>], cx: &mut AsyncApp) {
    for workspace in workspaces {
        let is_empty = match workspace
            .downgrade()
            .read_with(cx, |workspace, cx| workspace.root_paths(cx).is_empty())
        {
            Ok(is_empty) => is_empty,
            Err(_) => {
                log::debug!("Workspace entity already dropped during cleanup; skipping");
                continue;
            }
        };
        if !is_empty {
            continue;
        }

        let Some(window) = window_for_workspace_async(workspace, cx) else {
            continue;
        };

        let _ = window.update(cx, |multi_workspace, window, cx| {
            if !multi_workspace.remove(workspace, window, cx) {
                window.remove_window();
            }
        });
    }
}

pub async fn restore_worktree_via_git(
    row: &ArchivedGitWorktree,
    cx: &mut AsyncApp,
) -> Result<PathBuf> {
    // Step 1: Find the main repo entity
    let (main_repo, _temp_project) = find_or_create_repository(&row.main_repo_path, cx).await?;

    // Step 2: Handle path conflicts
    let worktree_path = &row.worktree_path;
    let app_state = current_app_state(cx).context("no app state available")?;
    let already_exists = app_state.fs.metadata(worktree_path).await?.is_some();

    let final_path = if already_exists {
        let worktree_directory =
            worktrees_directory_for_repo(&row.main_repo_path, DEFAULT_WORKTREE_DIRECTORY)?;
        let new_name = format!(
            "{}-restored-{}",
            row.branch_name.as_deref().unwrap_or("worktree"),
            row.id
        );
        let project_name = row
            .main_repo_path
            .file_name()
            .context("git repo must have a directory name")?;
        worktree_directory.join(&new_name).join(project_name)
    } else {
        worktree_path.clone()
    };

    // Step 3: Create detached worktree
    let rx = main_repo.update(cx, |repo, _cx| {
        repo.create_worktree_detached(final_path.clone(), row.unstaged_commit_hash.clone())
    });
    rx.await
        .map_err(|_| anyhow!("worktree creation was canceled"))?
        .context("failed to create worktree")?;

    // Step 4: Get the worktree's repo entity
    let (wt_repo, _temp_wt_project) = find_or_create_repository(&final_path, cx).await?;

    // Step 5: Mixed reset to staged commit (undo the "WIP unstaged" commit)
    let rx = wt_repo.update(cx, |repo, cx| {
        repo.reset(row.staged_commit_hash.clone(), ResetMode::Mixed, cx)
    });
    match rx.await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            let _ = wt_repo
                .update(cx, |repo, cx| {
                    repo.reset(row.unstaged_commit_hash.clone(), ResetMode::Mixed, cx)
                })
                .await;
            return Err(error.context("mixed reset failed while restoring worktree"));
        }
        Err(_) => {
            return Err(anyhow!("mixed reset was canceled"));
        }
    }

    // Step 6: Soft reset to parent of staged commit (undo the "WIP staged" commit)
    let rx = wt_repo.update(cx, |repo, cx| {
        repo.reset(format!("{}~1", row.staged_commit_hash), ResetMode::Soft, cx)
    });
    match rx.await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            let _ = wt_repo
                .update(cx, |repo, cx| {
                    repo.reset(row.unstaged_commit_hash.clone(), ResetMode::Mixed, cx)
                })
                .await;
            return Err(error.context("soft reset failed while restoring worktree"));
        }
        Err(_) => {
            return Err(anyhow!("soft reset was canceled"));
        }
    }

    // Step 7: Restore the branch
    if let Some(branch_name) = &row.branch_name {
        let rx = wt_repo.update(cx, |repo, _cx| repo.change_branch(branch_name.clone()));
        match rx.await {
            Ok(Ok(())) => {}
            _ => {
                let rx = wt_repo.update(cx, |repo, _cx| {
                    repo.create_branch(branch_name.clone(), None)
                });
                if let Ok(Err(_)) | Err(_) = rx.await {
                    log::warn!(
                        "Could not switch to branch '{}' — \
                         restored worktree is in detached HEAD state.",
                        branch_name
                    );
                }
            }
        }
    }

    Ok(final_path)
}

pub async fn cleanup_archived_worktree_record(row: &ArchivedGitWorktree, cx: &mut AsyncApp) {
    // Delete the git ref from the main repo
    if let Ok((main_repo, _temp_project)) = find_or_create_repository(&row.main_repo_path, cx).await
    {
        let ref_name = archived_worktree_ref_name(row.id);
        let rx = main_repo.update(cx, |repo, _cx| repo.delete_ref(ref_name));
        match rx.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => log::warn!("Failed to delete archive ref: {error}"),
            Err(_) => log::warn!("Archive ref deletion was canceled"),
        }
    }

    // Delete the DB records
    let store = cx.update(|cx| ThreadMetadataStore::global(cx));
    store
        .read_with(cx, |store, cx| store.delete_archived_worktree(row.id, cx))
        .await
        .log_err();
}

fn show_error_toast(summary: &str, detail: &str, plan: &CleanupPlan, cx: &mut AsyncApp) {
    let target_workspace = plan
        .current_workspace
        .clone()
        .or_else(|| plan.affected_workspaces.first().cloned());
    let Some(workspace) = target_workspace else {
        return;
    };

    let _ = workspace.update(cx, |workspace, cx| {
        struct ArchiveCleanupErrorToast;
        let message = if detail.is_empty() {
            summary.to_string()
        } else {
            format!("{summary}: {detail}")
        };
        workspace.show_toast(
            Toast::new(
                NotificationId::unique::<ArchiveCleanupErrorToast>(),
                message,
            )
            .autohide(),
            cx,
        );
    });
}

fn all_open_workspaces(cx: &App) -> Vec<Entity<Workspace>> {
    cx.windows()
        .into_iter()
        .filter_map(|window| window.downcast::<MultiWorkspace>())
        .flat_map(|multi_workspace| {
            multi_workspace
                .read(cx)
                .map(|multi_workspace| multi_workspace.workspaces().to_vec())
                .unwrap_or_default()
        })
        .collect()
}

fn workspace_survives(
    workspace: &Entity<Workspace>,
    doomed_roots: &HashSet<PathBuf>,
    cx: &App,
) -> bool {
    workspace
        .read(cx)
        .root_paths(cx)
        .into_iter()
        .any(|root| !doomed_roots.contains(root.as_ref()))
}

fn workspace_path_list(workspace: &Entity<Workspace>, cx: &App) -> PathList {
    PathList::new(&workspace.read(cx).root_paths(cx))
}

fn window_for_workspace(
    workspace: &Entity<Workspace>,
    cx: &App,
) -> Option<WindowHandle<MultiWorkspace>> {
    cx.windows()
        .into_iter()
        .filter_map(|window| window.downcast::<MultiWorkspace>())
        .find(|window| {
            window
                .read(cx)
                .map(|multi_workspace| multi_workspace.workspaces().contains(workspace))
                .unwrap_or(false)
        })
}

fn window_for_workspace_async(
    workspace: &Entity<Workspace>,
    cx: &mut AsyncApp,
) -> Option<WindowHandle<MultiWorkspace>> {
    let workspace = workspace.clone();
    cx.update(|cx| window_for_workspace(&workspace, cx))
}

fn current_app_state(cx: &mut AsyncApp) -> Option<Arc<AppState>> {
    cx.update(|cx| {
        all_open_workspaces(cx)
            .into_iter()
            .next()
            .map(|workspace| workspace.read(cx).app_state().clone())
    })
}

fn release_in_flight_roots(roots: &[RootPlan], cx: &mut AsyncApp) {
    cx.update_global::<ThreadArchiveCleanupCoordinator, _>(|coordinator, _cx| {
        let mut in_flight_roots = coordinator.in_flight_roots.lock();
        for root in roots {
            in_flight_roots.remove(&root.root_path);
        }
    });
}
