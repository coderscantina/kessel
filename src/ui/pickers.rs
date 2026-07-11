//! Proper grid pickers for bookmark / space appearance: an icon grid and a
//! color swatch row, embeddable inline or wrapped in a popover, replacing
//! the old dropdown selects. All builders are stateless — the host owns the
//! selected value and gets changes back through the callback.

use std::rc::Rc;

use gpui::prelude::*;
use gpui::{div, px, AnyElement, App, SharedString, Window};
use gpui_component::button::{Button, ButtonVariants as _};
use gpui_component::popover::Popover;
use gpui_component::{h_flex, ActiveTheme as _, Icon, IconName, Sizable as _};

use super::{bookmark_color, bookmark_icon, BOOKMARK_COLORS, BOOKMARK_ICONS};

/// Host callback: `None` means "default" (icon) / "none" (color).
pub type PickFn = Rc<dyn Fn(Option<String>, &mut Window, &mut App)>;

/// Inline grid of the named icons plus a leading "default" cell.
pub fn icon_grid(
    selected: Option<String>,
    default_icon: IconName,
    on_pick: PickFn,
) -> impl IntoElement {
    let mut cells: Vec<AnyElement> = Vec::new();
    for (ix, choice) in std::iter::once(None)
        .chain(BOOKMARK_ICONS.iter().map(|(name, _)| Some(*name)))
        .enumerate()
    {
        let icon = bookmark_icon(choice, default_icon.clone());
        let is_current = choice.map(str::to_string) == selected;
        let button = Button::new(("icon-cell", ix))
            .icon(icon)
            .small()
            .tooltip(choice.unwrap_or("Default"));
        let button = if is_current {
            button.primary()
        } else {
            button.ghost()
        };
        let on_pick = on_pick.clone();
        cells.push(
            button
                .on_click(move |_, window, cx| on_pick(choice.map(str::to_string), window, cx))
                .into_any_element(),
        );
    }
    h_flex().flex_wrap().gap_1().children(cells)
}

/// Inline swatch row over the named bookmark palette plus a leading "none"
/// cell.
pub fn color_grid(selected: Option<String>, on_pick: PickFn, cx: &App) -> impl IntoElement {
    let ring = cx.theme().primary;
    let none_border = cx.theme().border;
    let mut cells: Vec<AnyElement> = Vec::new();
    for (ix, choice) in std::iter::once(None)
        .chain(BOOKMARK_COLORS.iter().map(|(name, _)| Some(*name)))
        .enumerate()
    {
        let color = bookmark_color(choice);
        let is_current = choice.map(str::to_string) == selected;
        let on_pick = on_pick.clone();
        cells.push(
            div()
                .id(("color-cell", ix))
                .size(px(22.))
                .rounded_full()
                .cursor_pointer()
                .border_2()
                .border_color(if is_current {
                    ring
                } else {
                    gpui::transparent_black().into()
                })
                .p_0p5()
                .child(
                    div()
                        .size_full()
                        .rounded_full()
                        .when_some(color, |el, color| el.bg(color))
                        .when(color.is_none(), |el| el.border_1().border_color(none_border)),
                )
                .on_click(move |_, window, cx| on_pick(choice.map(str::to_string), window, cx))
                .into_any_element(),
        );
    }
    h_flex().flex_wrap().gap_1().children(cells)
}

/// Popover icon picker; the trigger button shows the current choice and
/// picking dismisses the popover.
pub fn icon_picker(
    id: &'static str,
    selected: Option<String>,
    default_icon: IconName,
    on_pick: PickFn,
) -> Popover {
    let current = bookmark_icon(selected.as_deref(), default_icon.clone());
    let label: SharedString = selected
        .clone()
        .map(Into::into)
        .unwrap_or_else(|| "Default".into());
    Popover::new(id)
        .trigger(
            Button::new("trigger")
                .icon(current)
                .label(label)
                .outline()
                .small(),
        )
        .content(move |_, _, cx| {
            let popover = cx.entity();
            let on_pick = on_pick.clone();
            let pick_and_dismiss: PickFn = Rc::new(move |value, window, cx| {
                on_pick(value, window, cx);
                popover.update(cx, |state, cx| state.dismiss(window, cx));
            });
            div().w(px(244.)).p_1().child(icon_grid(
                selected.clone(),
                default_icon.clone(),
                pick_and_dismiss,
            ))
        })
}

/// Popover color picker; the trigger shows the current swatch and picking
/// dismisses the popover.
pub fn color_picker(id: &'static str, selected: Option<String>, on_pick: PickFn) -> Popover {
    let label: SharedString = selected
        .clone()
        .map(Into::into)
        .unwrap_or_else(|| "None".into());
    let current = bookmark_color(selected.as_deref());
    Popover::new(id)
        .trigger(
            Button::new("trigger")
                .icon(match current {
                    Some(color) => Icon::default().path("icons/circle.svg").text_color(color),
                    None => Icon::default().path("icons/circle-off.svg"),
                })
                .label(label)
                .outline()
                .small(),
        )
        .content(move |_, _, cx| {
            let popover = cx.entity();
            let on_pick = on_pick.clone();
            let pick_and_dismiss: PickFn = Rc::new(move |value, window, cx| {
                on_pick(value, window, cx);
                popover.update(cx, |state, cx| state.dismiss(window, cx));
            });
            div()
                .p_1()
                .child(color_grid(selected.clone(), pick_and_dismiss, cx))
        })
}
