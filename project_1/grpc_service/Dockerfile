FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /app

# 1. Kopiraj samo .csproj fajl i uradi restore (ovo štedi vreme kroz Docker keš)
COPY *.csproj ./
RUN dotnet restore

# 2. KLJUČNI KORAK: Kopiraj SVE ostale fajlove (uključujući i tvoj Protos folder)
COPY . ./

# 3. Tek nakon što su fajlovi fizički tu, pokreni build i publish
RUN dotnet publish -c Release -o out --no-restore

# Stage 2: Runtime imidž koji pokreće aplikaciju
FROM mcr.microsoft.com/dotnet/aspnet:8.0
WORKDIR /app
COPY --from=build /app/out .
ENTRYPOINT ["dotnet", "grpc_service.dll"]