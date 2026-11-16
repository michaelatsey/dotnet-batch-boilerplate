using System.ComponentModel.DataAnnotations;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Configuration;

namespace Batch.Core.Parameters;

internal sealed record ParameterBindingResult(object? Value, IReadOnlyList<string> Errors)
{
    public bool IsValid => Errors.Count == 0 && Value is not null;

    public static ParameterBindingResult Failed(params IEnumerable<string> errors) => new(null, errors.ToList());
}

/// <summary>
/// Lie des paramètres « clé=valeur » (CLI, JobDataMap Quartz) vers un type fort, puis valide par DataAnnotations.
/// Réutilise le binder de Microsoft.Extensions.Configuration : mêmes conversions que appsettings.json
/// (DateOnly, TimeSpan, enums, listes via <c>Clé:0</c>, objets imbriqués via <c>Objet:Prop</c>).
/// </summary>
internal static class ParameterBinder
{
    public static ParameterBindingResult Bind<TParams>(IReadOnlyDictionary<string, string?> raw)
        where TParams : class, new()
    {
        TParams value;
        try
        {
            var configuration = new ConfigurationBuilder().AddInMemoryCollection(raw).Build();

            // Une clé inconnue est une faute de frappe dans l'ordonnanceur : on refuse plutôt que d'ignorer.
            value = configuration.Get<TParams>(o => o.ErrorOnUnknownConfiguration = true) ?? new TParams();
        }
        catch (InvalidOperationException ex)
        {
            return ParameterBindingResult.Failed(ex.Message);
        }

        var results = new List<ValidationResult>();
        if (!Validator.TryValidateObject(value, new ValidationContext(value), results, validateAllProperties: true))
        {
            return ParameterBindingResult.Failed(results.Select(Describe));
        }

        return new ParameterBindingResult(value, []);
    }

    private static string Describe(ValidationResult result)
    {
        var members = string.Join(",", result.MemberNames);
        return members.Length == 0 ? result.ErrorMessage ?? "Invalid value." : $"{members}: {result.ErrorMessage}";
    }
}

/// <summary>Empreinte stable d'un jeu de paramètres : clé de verrou, clé de reprise, audit.</summary>
internal static class ParameterFingerprint
{
    public static SortedDictionary<string, string?> Normalize(IReadOnlyDictionary<string, string?> raw)
    {
        var sorted = new SortedDictionary<string, string?>(StringComparer.Ordinal);
        foreach (var (key, value) in raw)
        {
            sorted[key.ToLowerInvariant()] = value;
        }

        return sorted;
    }

    public static string Hash(SortedDictionary<string, string?> normalized)
    {
        var builder = new StringBuilder();
        foreach (var (key, value) in normalized)
        {
            builder.Append(key).Append('=').Append(value).Append('\n');
        }

        var digest = SHA256.HashData(Encoding.UTF8.GetBytes(builder.ToString()));
        return Convert.ToHexStringLower(digest, 0, 16);
    }

    public static string ToJson(SortedDictionary<string, string?> normalized) => JsonSerializer.Serialize(normalized);
}
