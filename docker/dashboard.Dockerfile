# Dashboard container — .NET 10 Blazor Server app (multi-stage build).
# Serves the ESG dashboard on port 8080.

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY dashboard/ESG.Dashboard/ESG.Dashboard.csproj .
RUN dotnet restore

COPY dashboard/ESG.Dashboard/ .
RUN dotnet publish -c Release -o /app/publish

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /app/publish .

# Run as non-root
RUN groupadd -r appuser && useradd -r -g appuser -d /app appuser \
    && chown -R appuser:appuser /app
USER appuser

ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s --retries=3 \
  CMD curl -f http://localhost:8080/healthz || exit 1

ENTRYPOINT ["dotnet", "ESG.Dashboard.dll"]
