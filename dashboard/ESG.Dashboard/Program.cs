using ESG.Dashboard.Components;
using ESG.Dashboard.Data;
using ESG.Dashboard.Data.Repositories;
using ESG.Dashboard.Hubs;
using MudBlazor.Services;

var builder = WebApplication.CreateBuilder(args);

// Suppress JSDisconnectedException warnings from LightweightCharts disposal (temporarily disabled for debugging)
// builder.Logging.AddFilter("Microsoft.AspNetCore.Components.Server.Circuits.RemoteRenderer", LogLevel.Critical);
// builder.Logging.AddFilter("Microsoft.AspNetCore.Components.Server.Circuits.CircuitHost", LogLevel.Critical);

// MudBlazor
builder.Services.AddMudServices();

// In-memory cache (reduces DB load — scores change at most daily)
builder.Services.AddMemoryCache();

// Database
var connectionString = builder.Configuration.GetConnectionString("stoxx")
    ?? throw new InvalidOperationException("Connection string 'stoxx' not found.");
builder.Services.AddSingleton(new DbConnectionFactory(connectionString));
builder.Services.AddScoped<ScoresRepository>();
builder.Services.AddScoped<IndexPerformanceRepository>();
builder.Services.AddScoped<StockRepository>();
builder.Services.AddScoped<PulseRepository>();
builder.Services.AddScoped<IndexSelectionState>();
builder.Services.AddSingleton<CountryFlags>();

// SignalR — explicit timeouts to prevent silent circuit death
builder.Services.AddSignalR(options =>
{
    options.KeepAliveInterval = TimeSpan.FromSeconds(15);
    options.ClientTimeoutInterval = TimeSpan.FromSeconds(60);
    options.MaximumReceiveMessageSize = 64 * 1024; // 64 KB
});

// Blazor
builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents()
    .AddCircuitOptions(options => options.DetailedErrors = builder.Environment.IsDevelopment());

var app = builder.Build();

// Eagerly load country codes from DB into the singleton cache
await app.Services.GetRequiredService<CountryFlags>().EnsureLoadedAsync();

if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error", createScopeForErrors: true);
}

app.UseAntiforgery();
app.MapStaticAssets();

// Health check endpoint (Cloud Run cold start + load balancer readiness)
app.MapGet("/healthz", () => Results.Ok("ok"));

// SignalR hub
app.MapHub<PulseHub>("/hubs/pulse");

app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode();

app.Run();
