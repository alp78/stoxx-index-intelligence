using Microsoft.AspNetCore.SignalR;

namespace ESG.Dashboard.Hubs;

/// <summary>SignalR hub for real-time pulse data — clients subscribe/unsubscribe to index groups.</summary>
public class PulseHub : Hub
{
    public async Task SubscribeToIndex(string index)
    {
        await Groups.AddToGroupAsync(Context.ConnectionId, index);
    }

    public async Task UnsubscribeFromIndex(string index)
    {
        await Groups.RemoveFromGroupAsync(Context.ConnectionId, index);
    }
}

/// <summary>Lightweight DTO broadcast to subscribed clients on each price tick.</summary>
public record PulseMessage(string Index, string Symbol, double Price, double Change, DateTime Timestamp);
