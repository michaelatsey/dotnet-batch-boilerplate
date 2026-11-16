# ADR 0001 — Un moteur unique, indépendant de l'ordonnanceur

- Statut : accepté
- Date : 2026-11-16

## Contexte

Les jobs doivent pouvoir être lancés par un ordonnanceur d'entreprise (VTOM, Control-M), par Kubernetes, par cron, ou planifiés en interne par Quartz.

Les ordonnanceurs externes lancent un processus et lisent un code retour. Quartz appelle une méthode dans un processus long.

## Décision

Tout déclenchement passe par `IJobRunner.RunAsync`. Les jobs implémentent `IBatchJob<T>` ou `IChunkedJob<T, TIn, TOut>` et ne référencent ni Quartz ni la ligne de commande.

La CLI (`batch run`) et Quartz (`QuartzBatchJob`) sont des adaptateurs de quelques dizaines de lignes.

## Conséquences

- Un job se teste sans ordonnanceur, en appelant le runner.
- Changer d'ordonnanceur ne touche aucun job.
- Le verrou, le journal, la reprise et les codes retour sont identiques quel que soit le déclencheur.
- Coût : une indirection (descripteurs construits à l'enregistrement, sans réflexion à l'exécution).
