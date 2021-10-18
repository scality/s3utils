# S3utils Release Plan

## Docker Image Generation

Docker images are hosted on [registry.scality.com](registry.scality.com).
S3utils has two namespaces there:

* Production Namespace: registry.scality.com/s3utils
* Dev Namespace: registry.scality.com/s3utils-dev

With every CI build, the CI will push images, tagging the
content with the developer branch's short SHA-1 commit hash.
This allows those images to be used by developers, CI builds,
build chain and so on.

Tagged versions of s3utils will be stored in the production namespace.

## How to Pull Docker Images

```sh
docker pull registry.scality.com/s3utils-dev/s3utils:<commit hash>
docker pull registry.scality.com/s3utils/s3utils:<tag>
```

## Release Process

To release a production image:

* Name the tag for the repository and Docker image.

* Use the `yarn version` command with the same tag to update `package.json`.

* Create a PR and merge the `package.json` change.

* Run the workflow [action release](https://github.com/scality/s3utils/actions/workflows/release.yml) using
  * A given branch that ideally matches the tag.
  * Fill the field `tag` with the actual tag
  * Fill the field `prerelease` with true if it's one
