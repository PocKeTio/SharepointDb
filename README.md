Voici la **liste précise (dev)** de ce qui doit être **développé / corrigé / confirmé**, avec **règles + périmètre exact** (d’après le transcript).

---

## 1) Périmètre des feuilles (à confirmer + action)

### À garder (utilisées)

* **Moorea**
* **SG29**
* **Sogelife**
* **SLAB BVE**
* **SGCIB**

### À retirer

* **SG New York** : plus de fichiers reçus → **peut être supprimé du flux d’import + du code associé**

---

## 2) Bouton **Importer** (à corriger)

### Fonction attendue

* Importer les fichiers depuis **Outlook** et alimenter la macro **dans les bons onglets**.

### Bug actuel

* L’import **ne remplit rien** pour **SLAB BVE** (et “VE / SlabGE” mentionné comme non alimenté).

### À faire (tech)

* Vérifier le mapping “source → feuille” pour **SLAB BVE** (et l’autre feuille VE si distincte dans le code).
* Vérifier que l’import s’exécute jusqu’au bout (pas d’arrêt silencieux / filtrage qui exclut).
* Livrable test : après clic sur *Importer*, **les lignes apparaissent bien** dans SLAB BVE (comme pour les “3 premiers”).

---

## 3) Bouton **Respond as SGCIB** = récupération commentaires (à modifier)

### Fonction réelle attendue

* **Rapatrier les commentaires** saisis côté “collaborateur” vers **SGCIB**, dans **la colonne Y** (explicitement : “les commentaires viennent se mettre dans la colonne Y”).
* Le mail Outlook généré par ce bouton **n’est pas exploité**.

### À faire

* **Supprimer ou désactiver la génération de mail** (ou la rendre optionnelle), conserver uniquement la logique “copie commentaires → SGCIB col Y”.
* Confirmer la logique de jointure : apparemment via une référence type **GFRPPAGY** (à identifier dans le code : clé de matching contrepartie/ordre).

### Bug critique à corriger

* Les **commentaires J-1** ne sont pas repris quand **une autre personne** lance la macro :

  * Si **même collaborateur** lance plusieurs jours d’affilée → OK
  * Si **collaborateur différent** lance le lendemain → les commentaires “sautent”

### Comportement attendu

* À chaque run, la macro doit **recharger les commentaires depuis l’archive J-1**, **indépendamment** de l’utilisateur Windows/Excel qui lance.

---

## 4) Bouton **Générer le reporting** (mail management) (à modifier)

### Problème actuel

* Le reporting inclut des statuts **Completed Settled** / **Completed Rejected** (post migration e-settlement) + des opérations anciennes (ex : **2025**) → bruit.

### Règle attendue (précise)

* Le mail de reporting management doit inclure **uniquement** :

  * **Pending**
  * **Processing** (optionnel mais accepté : “à la limite pending et processing”)
* Exclure tout le reste, en particulier :

  * **Completed Settled**
  * **Completed Rejected**

### Détail important

* L’utilisateur supprime manuellement les lignes dans le mail, mais **la suppression dans Excel ne se reflète pas dans le mail** → donc la bonne solution est **filtrage en amont** lors de la génération.

---

## 5) Filtres de données (à implémenter)

### Filtre de date (prioritaire)

* Appliquer un filtre automatique pour ne garder que **les 3 dernières semaines** vs date du jour.
* Objectif : ne plus voir des opérations **2025** qui “polluent”.

### Couleurs (à ne pas casser)

* Conserver le comportement existant :

  * **dates passées** → lignes **blanches**
  * **dates futures** → lignes **surlignées** (elle dit “doit rester en …” = mise en évidence)

> Donc : le filtre + nettoyage ne doivent pas supprimer / casser la logique de coloration.

---

## 6) Génération de mails “Pending” depuis sélection + clic (à réparer + refactor)

### Constat actuel

* Le mail se génère mais est **vide**
* L’adresse destinataire “a sauté” : avant ça récupérait depuis onglet **Destinataire** (ex : “si je mets Optiver, c’était censé prendre… B39”).

### Flux utilisateur attendu (précis)

1. L’utilisateur est dans **Pending**
2. Il **filtre/analyse**
3. Il sélectionne une **contrepartie** via la colonne **AG** (il peut y en avoir plusieurs)
4. Il clique sur la cellule de la colonne **K** (statut/action) pour générer le mail

   * Important : elle précise que ce n’est **pas** un double-clic sur O/N qui génère, mais bien l’action côté **K** après filtre/choix.

### Types de mail attendus

* Support minimum demandé : **CLAC** et **CMIS/SEMIS**

  * CLAC = short / non livré (texte spécifique “pending short … can you provide an ETA”)
  * CMIS/SEMIS = contrepartie “not in place” (texte différent)
* Les statuts ont changé : maintenant il faut lire les infos dans :

  * **colonne O** pour CLAC (souvent)
  * **colonne N** pour SEMIS/CMIS (souvent)
    → À corriger : la logique doit détecter le type en fonction de la présence/valeur dans **N vs O** (ou du filtre appliqué auparavant).

### Pop-up “destinataire” (à corriger + étendre)

* Aujourd’hui il y a un pop-up incohérent (“couac dans le choix”).
* Elle demande explicitement **3 choix** :

  1. **Contrepartie**
  2. **Dépositaire**
  3. **Gestion**
* Règles associées :

  * **Contrepartie** : mail standard (anglais OK, 99% des cas)
  * **Gestion** : conserver texte existant si retrouvé (cas “prévenir le gérant”)
  * **Dépositaire** : aujourd’hui mail sans texte → ajouter au minimum une phrase type :

    * “Bonjour, pourriez-vous nous apporter plus d’informations sur cette opération ?”
    * * inclure le tableau récap

### Destinataires (à restaurer)

* Restaurer la résolution email via l’onglet **Destinataire** (mapping contrepartie → email).
* Le mapping était basé sur une cellule précise (ex “B39”) → à revalider via la structure actuelle, mais l’exigence = **ça doit remplir automatiquement le champ To**.

### Détails tableau

* Elle mentionne avoir fourni un template “avec plus de colonnes” pour le cas back-office/dépositaire :

  * => inclure davantage de colonnes dans le tableau pour Dépositaire (et éventuellement Gestion) selon ce template.

---

## 7) Commentaire automatique sur “matché” (à ajouter)

### Règle exacte

* Pour les opérations **matchées** avec **sous-statut vide** :

  * ajouter automatiquement un commentaire : **“Opération matchée”**
* Si matché **avec sous-statut** (ex : “matcher clac”) :

  * **ne pas auto-commenter**

### Écriture des commentaires

* Ne jamais écraser : **concaténer** (ajouter une nouvelle ligne / entrée)
* Elle veut potentiellement un commentaire **chaque jour** : “matché” aujourd’hui, puis “matché” demain si toujours matché.

