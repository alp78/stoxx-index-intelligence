namespace ESG.Dashboard.Data;

/// <summary>
/// Scoped service that holds the user's selected index across page navigations.
/// In Blazor Server each browser tab gets its own scoped service instance,
/// so the selection persists for the tab's lifetime.
/// </summary>
public class IndexSelectionState
{
    public string SelectedIndex { get; set; } = "euro_stoxx_50";
}
