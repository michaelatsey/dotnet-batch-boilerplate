namespace Batch.Abstractions;

/// <summary>
/// Optionnel : déclare l'identité de la donnée d'entrée d'une exécution (empreinte du fichier, version
/// de la source...). À implémenter par tout job dont les paramètres ne suffisent pas à désigner la donnée,
/// typiquement un fichier toujours déposé au même chemin.
/// </summary>
/// <remarks>
/// <para>
/// Le moteur la calcule avant de démarrer et la journalise. <c>--restart</c> ne reprend un checkpoint,
/// ou ne conclut « rien à faire », que si l'identité est la même que celle de la dernière exécution.
/// Sinon le run démarre à neuf : un checkpoint n'a de sens que sur la donnée qui l'a produit.
/// </para>
/// <para>
/// Si le calcul échoue, le run démarre aussi à neuf (rejouer est sûr, l'écriture étant idempotente ;
/// sauter des lignes ne l'est pas), et l'erreur réelle remonte ensuite de la lecture.
/// </para>
/// <para>Appelé dans son propre scope DI, avant le journal : pas d'effet de bord, coût raisonnable.</para>
/// </remarks>
public interface IInputIdentityProvider<in TParams>
    where TParams : class, new()
{
    ValueTask<string> ComputeInputIdentityAsync(TParams parameters, CancellationToken cancellationToken);
}
