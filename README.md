# catalog-checker
A tool to check the integrity of container images referred to in an [OLM file-based catalog](https://olm.operatorframework.io/docs/reference/file-based-catalogs/)

# How to

## Required inputs:
- a valid pull secret / registry token to the registry the images are referenced from (see https://access.redhat.com/terms-based-registry/ for registry.redhat.io)
- an OLM catalog JSON file, which you can obtain via [opm](https://github.com/operator-framework/operator-registry/releases): 

```
opm render registry.redhat.io/redhat/redhat-operator-index:v4.13 > redhat-operator-index-v4.13.json
```

## run like this:

```
go run main.go --csv redhat-operator-index-v4.13-errors.csv --parallel 24 --catalog redhat-operator-index-v4.13.json
```

Existing docker authentication will be picked up from the known paths.

## Supported flags:

```
Flags:
      --auth string      Path to Docker auth file
      --catalog string   Path to the file-based catalog JSON file
      --csv string       Path to a CSV file to write the results to
  -h, --help             help for catalog-checker
      --parallel int     Number of parallel checks
      --show-errors      Whether to output errors on
```
