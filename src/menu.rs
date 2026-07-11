//! Native application menu bar (macOS main menu; title-bar menus elsewhere).
//!
//! Menu items dispatch the same gpui actions the keymap uses, through the
//! focused view — so File → New Tab hits `workspace::NewTab` on the root and
//! Go → Back hits `file_pane::GoBack` on the focused pane, exactly like the
//! keyboard shortcuts. Rebuilt (`install`) whenever the locale changes.

use gpui::{App, Menu, MenuItem, OsAction, SystemMenuType};
use gpui_component::input;

use crate::app::{
    About, CheckForUpdates, CloseTab, Help, LayoutSingle, LayoutSplitH, LayoutSplitV, NewTab,
    NextTab, OpenBookmarks, OpenPreferences, OpenThemeBrowser, PrevTab, Quit, SyncPanes,
    ToggleCommandBar, ToggleLogs, ToggleSidebar, ToggleTransfers,
};
use crate::i18n::t;
use crate::ui::file_pane;

pub fn install(cx: &mut App) {
    cx.set_menus(vec![
        // The application menu; macOS titles it with the bundle name.
        Menu {
            name: "kessel".into(),
            items: vec![
                MenuItem::action(t("menu.app.about"), About),
                MenuItem::action(t("menu.app.check_updates"), CheckForUpdates),
                MenuItem::separator(),
                MenuItem::action(t("menu.app.preferences"), OpenPreferences),
                MenuItem::separator(),
                MenuItem::os_submenu(t("menu.app.services"), SystemMenuType::Services),
                MenuItem::separator(),
                MenuItem::action(t("menu.app.quit"), Quit),
            ],
        },
        Menu {
            name: t("menu.file"),
            items: vec![
                MenuItem::action(t("menu.file.new_tab"), NewTab),
                MenuItem::action(t("menu.file.close_tab"), CloseTab),
                MenuItem::separator(),
                MenuItem::action(t("menu.file.bookmarks"), OpenBookmarks),
                MenuItem::action(t("menu.file.new_folder"), file_pane::NewFolder),
                MenuItem::separator(),
                MenuItem::action(t("menu.file.synchronize"), SyncPanes),
            ],
        },
        // Standard OS actions so text fields get the native Edit behavior.
        Menu {
            name: t("menu.edit"),
            items: vec![
                MenuItem::os_action(t("menu.edit.undo"), input::Undo, OsAction::Undo),
                MenuItem::os_action(t("menu.edit.redo"), input::Redo, OsAction::Redo),
                MenuItem::separator(),
                MenuItem::os_action(t("menu.edit.cut"), input::Cut, OsAction::Cut),
                MenuItem::os_action(t("menu.edit.copy"), input::Copy, OsAction::Copy),
                MenuItem::os_action(t("menu.edit.paste"), input::Paste, OsAction::Paste),
                MenuItem::os_action(
                    t("menu.edit.select_all"),
                    input::SelectAll,
                    OsAction::SelectAll,
                ),
            ],
        },
        Menu {
            name: t("menu.view"),
            items: vec![
                MenuItem::action(t("menu.view.command_bar"), ToggleCommandBar),
                MenuItem::separator(),
                MenuItem::action(t("menu.view.theme_browser"), OpenThemeBrowser),
                MenuItem::separator(),
                MenuItem::action(t("menu.view.toggle_sidebar"), ToggleSidebar),
                MenuItem::action(t("menu.view.toggle_transfers"), ToggleTransfers),
                MenuItem::action(t("menu.view.toggle_logs"), ToggleLogs),
                MenuItem::separator(),
                MenuItem::action(t("menu.view.layout_single"), LayoutSingle),
                MenuItem::action(t("menu.view.layout_split_h"), LayoutSplitH),
                MenuItem::action(t("menu.view.layout_split_v"), LayoutSplitV),
                MenuItem::separator(),
                MenuItem::action(t("menu.view.toggle_hidden"), file_pane::ToggleHidden),
                MenuItem::action(t("menu.view.refresh"), file_pane::Refresh),
            ],
        },
        Menu {
            name: t("menu.go"),
            items: vec![
                MenuItem::action(t("menu.go.back"), file_pane::GoBack),
                MenuItem::action(t("menu.go.forward"), file_pane::GoForward),
                MenuItem::action(t("menu.go.up"), file_pane::GoUp),
                MenuItem::separator(),
                MenuItem::action(t("menu.go.next_tab"), NextTab),
                MenuItem::action(t("menu.go.prev_tab"), PrevTab),
            ],
        },
        Menu {
            name: t("menu.help"),
            items: vec![MenuItem::action(t("menu.help.docs"), Help)],
        },
    ]);
}
