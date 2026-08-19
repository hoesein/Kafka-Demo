FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

COPY ["KafkaWorkerPlatform.sln", "."]
COPY ["src/Worker.Api/Worker.Api.csproj", "src/Worker.Api/"]
COPY ["src/Worker.Contracts/Worker.Contracts.csproj", "src/Worker.Contracts/"]
COPY ["src/Worker.Core/Worker.Core.csproj", "src/Worker.Core/"]
COPY ["src/Worker.Host/Worker.Host.csproj", "src/Worker.Host/"]
COPY ["src/Worker.Infrastructure/Worker.Infrastructure.csproj", "src/Worker.Infrastructure/"]
COPY ["src/Worker.Plugins/Worker.Plugins.csproj", "src/Worker.Plugins/"]
COPY ["tests/Worker.UnitTests/Worker.UnitTests.csproj", "tests/Worker.UnitTests/"]

RUN dotnet restore "KafkaWorkerPlatform.sln"

COPY . .
WORKDIR "/src/src/Worker.Host"
RUN dotnet publish "Worker.Host.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS final
WORKDIR /app
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "Worker.Host.dll"]
