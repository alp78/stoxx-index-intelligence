using ESG.Dashboard.Components;
using ESG.Dashboard.Data;
using ESG.Dashboard.Data.Repositories;
using ESG.Dashboard.Hubs;
using MudBlazor.Services;

var builder = WebApplication.CreateBuilder(args);

// Suppress JSDisconnectedException warnings from LightweightCharts disposal
builder.Logging.AddFilter("Microsoft.AspNetCore.Components.Server.Circuits.RemoteRenderer", LogLevel.Critical);
builder.Logging.AddFilter("Microsoft.AspNetCore.Components.Server.Circuits.CircuitHost", LogLevel.Critical);

// MudBlazor
builder.Services.AddMudServices();

// Database
var connectionString = builder.Configuration.GetConnectionString("stoxx")
    ?? throw new InvalidOperationException("Connection string 'stoxx' not found.");
builder.Services.AddSingleton(new DbConnectionFactory(connectionString));
builder.Services.AddScoped<ScoresRepository>();
builder.Services.AddScoped<IndexPerformanceRepository>();
builder.Services.AddScoped<StockRepository>();
builder.Services.AddScoped<PulseRepository>();
builder.Services.AddScoped<IndexSelectionState>();

// SignalR
builder.Services.AddSignalR();

// Blazor
builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();

var app = builder.Build();

if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error", createScopeForErrors: true);
}

app.UseAntiforgery();
app.MapStaticAssets();

// SignalR hub
app.MapHub<PulseHub>("/hubs/pulse");

app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode();

app.Run();
