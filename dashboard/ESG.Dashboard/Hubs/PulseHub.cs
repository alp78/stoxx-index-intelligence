using Microsoft.AspNetCore.SignalR;

namespace ESG.Dashboard.Hubs;

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

public record PulseMessage(string Index, string Symbol, double Price, double Change, DateTime Timestamp);
