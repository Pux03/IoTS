# Stage 1: Build
FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /app

# Kopiraj fajlove i restore-uj pakete
COPY *.csproj ./
RUN dotnet restore

# Kopiraj sve ostalo i build-uj
COPY . ./
RUN dotnet publish -c Release -o out

# Stage 2: Run
FROM mcr.microsoft.com/dotnet/aspnet:9.0
WORKDIR /app
COPY --from=build /app/out .

# Ekspozuj port na kom radi .NET
EXPOSE 8080
ENTRYPOINT ["dotnet", "rest_service.dll"]