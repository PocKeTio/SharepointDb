01-16 Réunion : Macro Excel pour Transactions Financières-Notes informatiques
Voici le résumé de votre réunion, organisé selon le modèle fourni.
✅ Points Clés & Statut
La discussion a porté sur les ajustements et les correctifs à apporter à une macro Excel utilisée pour traiter et importer des données de fichiers reçus par email. Il s'agit d'une revue détaillée des fonctionnalités existantes et des améliorations souhaitées.
   Ce qui a été fait : Gianni a livré une version de la macro sur laquelle Estelle a effectué des tests et préparé une liste de retours.
   Plans pour aujourd'hui/prochainement : Gianni va travailler sur l'implémentation des modifications demandées par Estelle. Celles-ci incluent des ajustements sur l'interface (colonnes à masquer, ajout et réorganisation de colonnes), la logique des filtres (statuts à exclure/inclure), la correction de bugs sur l'import de certains fichiers, et l'amélioration de la fonctionnalité de mailing. Estelle, de son côté, enverra par email les détails nécessaires (format de mail, fichiers de test).
   Bloqueurs : L'import des fichiers pour les onglets "VE" et "Slab" ne fonctionne pas correctement ; les onglets restent vides ou affichent des données incorrectes. Gianni a besoin de fichiers de test pour investiguer. La fonctionnalité de mailing n'est pas encore optimale et nécessite des ajustements sur le contenu et le format du mail généré.
🗓️ Timelines & Milestones
   Les rejets ("completed rejected") doivent être conservés sur une semaine glissante (J-7).
🎯 Tableau des Actions
| Tâche | Assigné à | Deadline | Notes |
| :--- | :--- | :--- | :--- |
| Supprimer l'onglet "Moorea Equity". | Gianni | Aucune | L'onglet n'est plus nécessaire car les données sont maintenant incluses dans un autre fichier. |
| Renommer l'identifiant "SG29" en "SGIS" dans le code si possible. | Gianni | Aucune | Changement de nom interne depuis l'année dernière. |
| Masquer les colonnes de C à I par défaut via la macro. | Gianni | Aucune | Pour améliorer la lisibilité. |
| Réorganiser les colonnes "Quantité" et ajouter "Type de quantité", "Quantité restante à dénouer" et "Quantité totale dénouée" après la colonne V. | Gianni | Aucune | Estelle a inséré un exemple directement dans le fichier pour servir de modèle. |
| Ajouter une colonne "Devise" après la colonne "Montant". | Gianni | Aucune | L'information de devise est manquante. |
| Ajuster les filtres de statut : conserver les "processing", supprimer les "completed settle" et "completed settle after partial", mais conserver les "completed partialized settle". | Gianni | Aucune | Le but est de n'afficher que les opérations encore en suspens ou partiellement traitées. |
| Mettre en place un filtre d'antériorité pour n'afficher les "completed / rejected" que sur une période de 7 jours glissants (J-7). | Gianni | Aucune | Pour éviter la pollution visuelle avec d'anciennes opérations clôturées. |
| Corriger le bug d'import pour l'onglet "VE" (fichier "Tommy/Priv"). | Gianni | Aucune | Estelle enverra le mail contenant le fichier pour test. |
| Corriger le bug d'import pour l'onglet "Slab" (fichiers "GSP Priv"). | Gianni | Aucune | Les données n'apparaissent pas ou sont incorrectes. Estelle renverra le mail avec les fichiers. |
| Améliorer le format du mail généré pour inclure les données spécifiées (référence SG, type de transaction, VD, TD, ISIN, montant, devise, etc.). | Gianni | Aucune | Estelle enverra le format exact souhaité par email. |
| Configurer le texte du mail pour qu'il soit automatiquement en anglais pour les contreparties. | Gianni | Aucune | Estelle fournira le texte exact. Un texte différent sera défini plus tard pour les dépositaires. |
| Dans l'onglet "Slab", ajouter une colonne pour le "sens de l'opération" provenant de la colonne F des fichiers GSP Priv. | Gianni | Aucune | Demande des collègues pour avoir une information manquante. |
| Envoyer un mail à Gianni avec les fichiers de test ("VE" et "Slab") et les détails pour le format du mailing. | Estelle | Aucune | Pour permettre à Gianni de corriger les bugs et d'implémenter les améliorations. |
➡️ Actions de Suivi
   Estelle doit réfléchir au texte du mail à envoyer aux dépositaires et le communiquer plus tard à Gianni.
   Estelle testera l'ensemble des fonctionnalités (notamment le bouton "répondre à SGCIB") la semaine prochaine lorsqu'elle sera en charge des "Pending".
❗️ Éléments en Suspens & Risques
   Risques Potentiels :
       L'import des données des onglets "VE" et "Slab" est actuellement non fonctionnel, ce qui bloque le traitement de ces fichiers.
       Le filtre de statut actuel inclut des opérations terminées ("completed"), ce qui pollue la vue des opérations nécessitant une action.
   Discussions non terminées :
       Le texte exact et la logique pour les mails destinés aux dépositaires n'ont pas été finalisés et seront définis ultérieurement.
       La discussion sur l'ajout d'une pièce jointe PDF au mail a été mentionnée comme "compliquée" et mise en attente. Estelle a indiqué que ce n'était pas une priorité si c'était trop complexe à réaliser pour le moment.
