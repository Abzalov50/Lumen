# Connexions PostgreSQL

## Objectif

Le pool natif de Lumen doit empêcher qu’une socket PostgreSQL distante
expirée soit transmise au traitement applicatif.

`Postmodern:connected-p` indique seulement si l’objet de connexion est encore
considéré comme ouvert localement. Après une coupure réseau, une expiration
NAT ou une fermeture côté serveur, cette information peut rester vraie alors
que le prochain échange échouera.

## Validation lors de l’emprunt

Lorsqu’une connexion existante est empruntée au pool :

1. Lumen vérifie son état local ;
2. Lumen exécute `SELECT 1` sur la socket ;
3. en cas d’échec, la connexion et ses plans préparés sont détruits ;
4. une nouvelle connexion est créée avant l’appel du traitement applicatif.

Une connexion qui vient d’être créée n’est pas sondée une seconde fois : la
réussite de la négociation PostgreSQL constitue déjà sa validation initiale.
Lors du retour au pool, seul l’état local est contrôlé afin d’éviter un second
aller-retour réseau par requête.

## Garanties de rejeu

La validation intervient avant l’appel du traitement applicatif. Lumen ne
rejoue donc pas automatiquement un contrôleur, une transaction ou une
opération d’écriture lorsque la connexion tombe après le début du traitement.

Une erreur réseau pendant le traitement provoque l’élimination de la
connexion. La couche appelante reçoit l’erreur et décide explicitement si
l’opération est rejouable.

## Messages reconnus

Le classificateur `db-network-error-p` couvre notamment :

- `Connection to database server lost` ;
- `Database server connection lost` ;
- `Connection is closed` ;
- `Connection not open` ;
- les fermetures de socket, ruptures de tube, réinitialisations et délais
  réseau déjà pris en charge.

## Cas de test de référence

Le test d’intégration :

1. emprunte une connexion et relève son `pg_backend_pid()` ;
2. la rend au pool ;
3. termine exclusivement ce backend depuis une connexion de contrôle ;
4. emprunte de nouveau une connexion ;
5. vérifie que `SELECT 42` réussit et que le traitement applicatif n’est appelé
   qu’une seule fois.

Si le rôle de test ne peut pas terminer sa propre connexion PostgreSQL, ce
scénario est ignoré explicitement ; les tests purs du classificateur restent
exécutés.
