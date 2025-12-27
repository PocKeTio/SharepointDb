# SharePointDb.Sample (exemple)

Ce projet est un **exécutable console net48** qui sert de **harness de test** et d’**exemple d’intégration**.

- Il référence les DLL de la solution :
  - `SharePointDb.Core`
  - `SharePointDb.Sqlite`
  - `SharePointDb.SharePoint`
  - `SharePointDb.Sync`
  - `SharePointDb.Auth.WinForms` (auth interactive via WebView2)
- Il expose une façade simple : `SharePointDbClient`.

## 1) Prérequis (important)

### 1.1 Migration / Provisioning SharePoint
Avant de pouvoir synchroniser, il faut que les listes système et la configuration existent côté SharePoint :

- `APP_Config`
- `APP_Tables`

Le projet `SharePointDb.Migration` sert à :
- créer/assurer l’existence de ces listes
- créer les champs requis
- seed une configuration initiale

> En production, la **migration** est typiquement une étape “Ops” (une fois par environnement / version), séparée de l’application.

### 1.2 Configuration `APP_Tables`
`SharePointDb.Sync` se base sur la config lue depuis `APP_Tables`.
Chaque table doit avoir notamment :
- `EntityName` (nom logique)
- `ListId` / infos de la liste SharePoint
- `PkInternalName` (champ SharePoint de la PK applicative, typiquement `AppPK`)
- `SelectFields` (liste de champs à mettre dans le miroir SQLite)
- `Enabled = true`

## 2) Utiliser le projet Sample (CLI)

Le binaire prend :
- `--site` (obligatoire) : URL du site SharePoint
- `--appId` (optionnel, défaut `APP`)
- `--sqlite` (optionnel, défaut `SharePointDb.Sample.sqlite`)
- `--cmd` (optionnel)

### Commandes
- `config`
- `sync-on-open`
- `sync-all`
- `sync-table --entity <EntityName>`
- `get --entity <EntityName> --pk <AppPK>`
- `enqueue-insert --entity <EntityName> --pk <AppPK> [--title <text>] [--value <text>]`
- `enqueue-update --entity <EntityName> --pk <AppPK> [--title <text>] [--value <text>]`
- `enqueue-delete --entity <EntityName> --pk <AppPK>`
- `recent-conflicts [--max <N>]`

### Exemples

1) Sync “à l’ouverture” (recommandé pour démarrer)
```txt
SharePointDb.Sample --site https://sharepoint/sites/MonSite/ --cmd sync-on-open
```

2) Sync d’une table précise
```txt
SharePointDb.Sample --site https://sharepoint/sites/MonSite/ --cmd sync-table --entity Clients
```

3) Lecture locale (après un sync)
```txt
SharePointDb.Sample --site https://sharepoint/sites/MonSite/ --cmd get --entity Clients --pk 12345
```

4) Écriture locale + outbox (puis sync)
```txt
SharePointDb.Sample --site https://sharepoint/sites/MonSite/ --cmd enqueue-insert --entity Clients --pk 12345 --title "Client 12345"
SharePointDb.Sample --site https://sharepoint/sites/MonSite/ --cmd sync-table --entity Clients
```

## 3) Intégrer dans une application (DLL) – le plus simple possible

### 3.1 Le principe
Dans ton application (WinForms/WPF/etc.), tu ne dois pas faire d’appels CMD.
Tu dois :
- référencer les projets (DLL)
- instancier un “client/facade” qui encapsule le moteur de sync

Le fichier `SharePointDbClient.cs` (dans ce projet sample) est un exemple prêt à l’emploi.

### 3.2 Exemple minimal d’intégration (copier/coller)

```csharp
using System;
using System.Threading.Tasks;
using SharePointDb.Auth.WinForms;
using SharePointDb.Sample;

public sealed class MyAppSharePointDb
{
    private readonly SharePointDbClient _client;

    public MyAppSharePointDb(Uri siteUri, string appId, string sqliteFile)
    {
        var cookieProvider = new WebView2CookieProvider();
        var options = new SharePointDbClientOptions(siteUri, appId, sqliteFile);
        _client = new SharePointDbClient(options, cookieProvider);
    }

    public Task InitializeAsync()
    {
        return _client.InitializeAsync();
    }

    // Exemple “sync-on-demand” : tu sync la table juste avant de la lire.
    public async Task<string> GetClientJsonAsync(string appPk)
    {
        await _client.SyncTableAsync("Clients");
        var row = await _client.GetLocalAsync("Clients", appPk);
        return row == null ? null : SharePointDb.Core.Json.Serialize(row);
    }
}
```

### 3.3 Où mettre la sync “sync-on-open” vs “sync-table” ?

- **`SyncOnOpenAsync()`** : au démarrage de l’app (ou ouverture de session/utilisateur)
  - fait un `SyncUp` puis `SyncDown` sur les tables configurées en `OnOpen`
- **`SyncTableAsync(entity)`** : juste avant d’accéder à une table (écran, repository, etc.)
  - fait un `SyncUp` puis `SyncDown` uniquement sur la table demandée
  - dans `SharePointDbClient`, il y a un **lock par table** pour éviter les sync concurrentes sur la même entité

### 3.4 Écritures : pattern recommandé
Quand l’utilisateur modifie des données :
- tu écris dans SQLite (miroir)
- tu mets une entrée dans l’outbox (`ChangeLog`)
- tu lances la sync (immédiate ou différée)

Dans `SharePointDbClient` :
- `UpsertLocalAndEnqueueInsertAsync`
- `UpsertLocalAndEnqueueUpdateAsync`
- `MarkLocalDeletedAndEnqueueSoftDeleteAsync`

### 3.5 Champs `Title` / `Value`
Le sample utilise `Title` et optionnellement `Value` pour montrer l’outbox.
Dans ton vrai projet, remplace ces champs par ceux de ton modèle.

Important :
- pour que le champ soit **persisté dans le miroir SQLite**, il doit être présent dans `SelectFields` de la table.
- pour que le champ soit **envoyé à SharePoint**, il doit exister côté liste SharePoint.

## 4) Variante “encore plus clean” (option)
Si tu veux éviter de dépendre du namespace `SharePointDb.Sample` dans ton app :
- on peut déplacer `SharePointDbClient` / `SharePointDbClientOptions` dans un projet DLL dédié `SharePointDb.Client`
- `SharePointDb.Sample` et ton app référencent tous les deux cette DLL
