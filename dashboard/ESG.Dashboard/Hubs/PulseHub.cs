using Microsoft.AspNetCore.SignalR;

namespace ESG.Dashboard.Hubs;

/// <summary>SignalR hub for real-time pulse data — clients subscribe/unsubscribe to index groups.</summary>
public class PulseHub : Hub
{
    private static readonly HashSet<string> ValidIndices = new(StringComparer.OrdinalIgnoreCase)
    {
        "STOXX50E", "SX5E", "SXXP"
    };

    /// <summary>Adds the caller's connection to the specified index group for real-time updates.</summary>
    /// <param name="index">Index identifier to subscribe to.</param>
    public async Task SubscribeToIndex(string index)
    {
        if (string.IsNullOrWhiteSpace(index) || !ValidIndices.Contains(index))
            return;

        await Groups.AddToGroupAsync(Context.ConnectionId, index);
    }

    /// <summary>Removes the caller's connection from the specified index group.</summary>
    /// <param name="index">Index identifier to unsubscribe from.</param>
    public async Task UnsubscribeFromIndex(string index)
    {
        if (string.IsNullOrWhiteSpace(index) || !ValidIndices.Contains(index))
            return;

        await Groups.RemoveFromGroupAsync(Context.ConnectionId, index);
    }
}

/// <summary>Lightweight DTO broadcast to subscribed clients on each price tick.</summary>
public record PulseMessage(string Index, string Symbol, double Price, double Change, DateTime Timestamp);
