Convert TTL to DSA:

```nu
nix develop path:. -c zsh
spinta -o rdf_fetch_remote=true copy ~/dev/contrib/semic/DCAT-AP/releases/3.0.1/html/shacl/*.ttl -o .tmp/manifest.csv
spinta check --check-names .tmp/manifest.csv
spinta copy .tmp/manifest.csv -o .tmp/manifest.mmd
mmdc -i .tmp/manifest.mmd -o .tmp/manifest.svg
```

Convert DSA to TTL

```nu
spinta copy .tmp/manifest.csv -o .tmp/manifest.ttl
spinta -o uri_prefix=https://data.gov.lt/id/ copy .tmp/manifest.csv -o .tmp/manifest.ttl
spinta copy ~/dev/data/metadata/datasets/gov/rc/ar/text_with_coordinates/dsa.csv -o .tmp/manifest.ttl
spinta copy ~/dev/data/metadata/datasets/gov/rc/ar/text/dsa.csv -o .tmp/manifest.ttl

spinta copy ~/dev/data/metadata/datasets/gov/rc/ar/*/dsa.csv -o .tmp/manifest.ttl
spinta copy ~/dev/data/metadata/datasets/gov/rc/ar/*/dsa.csv --access=open -o .tmp/manifest_open.ttl
```

Generate TTL for all manifests in `~/dev/data/manifest` and `~/dev/data/metadata`.

A manifest is a `.csv` file whose header has `id`, `dataset`, `model`, `property` and `type` columns (in any order). Output goes to `.tmp/copy/{repo}/{dataset}/manifest.ttl`, where `{dataset}` is the file path without `.csv`, except `dsa.csv`, which is the namespace directory itself.

```nu
let repos = [
    "manifest"
    "metadata"
]
let threads = 4

def is-manifest [file: string]: nothing -> bool {
    let required = [id dataset model property type]
    try {
        let header = open --raw $file | lines | first | from csv --noheaders | first | values
        $required | all { $in in $header }
    } catch {
        false
    }
}

let jobs = $repos | each {|repo|
    let path = ($"~/dev/data/($repo)" | path expand)
    glob $"($path)/**/*.csv" --exclude [**/.git/** **/.tmp/** **/.github/**]
    | where {|file| is-manifest $file }
    | each {|file|
        let rel = ($file | str replace $"($path)/" '' | str replace -r '\.csv$' '')
        let dataset = if ($rel | path basename) == 'dsa' { $rel | path dirname } else { $rel }
        {src: $file, out: $".tmp/copy/($repo)/($dataset)/manifest.ttl"}
    }
} | flatten

let duplicates = $jobs | get out | uniq -d
if ($duplicates | is-not-empty) {
    error make {msg: $"Duplicate output paths: ($duplicates | str join ', ')"}
}

print $"Manifests found: ($jobs | length)"

let results = $jobs | par-each --threads $threads {|job|
    mkdir ($job.out | path dirname)
    let run = (^spinta copy $job.src -o $job.out | complete)
    {
        src: $job.src
        out: $job.out
        code: $run.exit_code
        error: ($run.stderr | lines | where {|line| $line =~ '^[A-Za-z]' } | last 1 | str join)
    }
}
#| took 13m35s

let failed = $results | where code != 0
let ok = ($results | length) - ($failed | length)
print $"Converted: ($ok) of ($results | length)"
#| Converted: 1357 of 1499
if ($failed | is-not-empty) {
    print "Failed:"
    for job in $failed {
        print $"  ($job.src): ($job.error)"
    }
}

# Aggregate failed conversions by error type, then by normalized message
let aggregated = $failed | each {|job|
    let parsed = ($job.error | str trim | parse -r '^(?<type>[A-Za-z]+): (?<detail>.*)$')
    let kind = if ($parsed | is-empty) { '(no error type)' } else { $parsed.0.type }
    let msg = if ($parsed | is-empty) { $job.error | str trim } else { $parsed.0.detail }
    let pattern = $msg
        | str replace -ra r#'(^|\s)'[^']*''# r#'$1'…''#
        | str replace -ra r#'\d+'# 'N'
    {kind: $kind, pattern: $pattern, src: $job.src}
}

print ""
if ($aggregated | is-empty) {
    print "No conversion errors"
} else {
    print $"Error summary: ($aggregated | length) errors of ($results | length) jobs"

    let kinds = $aggregated | get kind | uniq | each {|k|
        let rows = $aggregated | where kind == $k
        let patterns = $rows | get pattern | uniq | each {|pat|
            let prow = $rows | where pattern == $pat
            {count: ($prow | length), message: $pat, files: ($prow | get src)}
        } | sort-by count --reverse
        {kind: $k, count: ($rows | length), patterns: $patterns}
    } | sort-by count --reverse

    for k in $kinds {
        print $"($k.count) ($k.kind)"
        for p in $k.patterns {
            print $"    ($p.count) ($p.message)"
        }
    }
}

```

Output:

```
Error summary: 142 errors of 1499 jobs
100 PartialTypeNotFound
    100 Partial type can only be used for ref type.
21 UsageError
    21 Invalid URI '…' of '…': prefix '…' is not declared.
12 (no error type)
    2 there is no base model.
    2 prepare must be specified for an enum.
    1 to backref, please specify which one to use.
    1 not declared.
    1 if all parts of the composite property are present.
    1 prefix '…' is not declared.
    1 implemented yet.
    1 declared.
8 ValueError
    8 Can't write IRI with invalid characters as Turtle: '…'
1 InvalidName
    1 Invalid '…' namespace code name.
```
