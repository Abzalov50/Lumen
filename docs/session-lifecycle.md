# Sessions HTTP : cycle de vie et cas d'utilisation

## Contrat

Le middleware de session conserve les données dans PostgreSQL et peut mettre
en cache les lectures. L'expiration PostgreSQL reste autoritative : une entrée
du cache ne doit jamais vivre au-delà de `expires_at`.

Les clés écrites par `session-set!` sont normalisées en minuscules. Les
fonctions `session-get` et `session-del!` acceptent indifféremment une chaîne
ou un symbole et comparent les clés sans tenir compte de la casse.

Les données historiques peuvent avoir été décodées depuis une ancienne
version, contenir une entrée non cons ou une clé nulle. Les accesseurs ignorent
ces entrées au lieu de provoquer une erreur de type. Une clé de lecture nulle
retourne `NIL`. Une clé d'écriture nulle est refusée explicitement.

## Cache de lecture

`%session-cache-put` calcule l'échéance effective ainsi :

```text
min(expiration de la session, instant courant + TTL du cache)
```

Une lecture réussie ne prolonge pas cette échéance. Après expiration, l'entrée
est supprimée du cache et la session doit être relue en base. La requête SQL
retourne dans le même aller-retour les données et `expires_at`.

## Déconnexion

`session-del!` est idempotente. La suppression de `"user-id"` retire aussi une
clé historique équivalente telle que `:USER-ID`. Répéter la suppression ne
modifie pas les autres données de session et ne produit pas d'erreur.

## Cas d'utilisation de référence

1. Une session valide est servie depuis le cache avant son échéance.
2. Une session expirée en base ne reste pas authentifiée par un cache actif.
3. Les clés `"role"`, `:ROLE` et `"ROLE"` sont équivalentes à la lecture et à
   la suppression.
4. Une alist contenant `NIL` ou `(NIL . valeur)` reste lisible.
5. Deux appels successifs à `session-del!` donnent le même résultat.
6. `session-set!` remplace toutes les variantes historiques de la clé et
   assainit les entrées mal formées.

La suite FiveAM `:session` couvre les clés mixtes, les données historiques,
l'idempotence et la conservation de l'expiration autoritative.
