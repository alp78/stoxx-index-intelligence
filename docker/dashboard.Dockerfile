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

ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080

ENTRYPOINT ["dotnet", "ESG.Dashboard.dll"]
