# River

Machinery for compatibility with <https://github.com/replit/river>, the protocol behind the Workspace ↔ pid2 WebSocket (and in general, the protocol behind the Workspace ↔ * WebSocket).

Since the pid2 services are declared in TypeScript using [TypeBox](https://github.com/sinclairzx81/typebox/) in the main repl-it-web repository, and it would be terrible for everyone involved to force folks to follow this paradigm of declaring their services, types, and interfaces in a foreign repository, any other River servers will use gRPC for their protocol definition and implementation. The [`@replit/river-codegen`](https://www.npmjs.com/package/@replit/river-codegen) package can be used to compile the gRPC `.proto` files into a River-compatible TypeBox declaration that can be then packaged into an npm package that can be imported by the Workspace and consumed ergonomically.

This includes the necessary machinery to act as a client or server for River:

* As a River server, create a WebSocket server and the gRPC -> Python River codegen (similar to the protoc flow that generates the Python bindings).
* As a River client, create a WebSocket client and the JSON Schema -> Python River codegen.
  `python -m river.codegen client --output pkgs/river/river/schema.py --client-name Pid2Client pkgs/river/schema.json`
* If we need to create the client-side of a Python gRPC River server, we also need to generate the JSON schema from the .proto file, with this command:
  `python -m river.codegen server-schema --output pkgs/river/river/schema.py  pkgs/river/tests/client/proto/test.proto  && cat ./test_schema.json`

## Publishing

### Release Drafts
Pending releases are curated by [release-drafter/release-drafter](https://github.com/release-drafter/release-drafter) on the [Releases](https://github.com/replit/river-python/releases) page.

Maintainers can see the next `Draft` release, regenerated every time [release-drafter.yml](https://github.com/replit/river-python/actions/workflows/release-drafter.yml) is triggered.

### PR Labeling

PRs merged since the last release are considered, with the labels on those PRs used for release metadata. `feature`, `bug`, `chore`, and `dependencies` are used for categorization, `major`, `minor`, and `patch` are used to influence the next release's version.

These labels can be altered after merge, re-trigger release-drafter to get it to regenerate the draft once you've curated the next release.

### Triggering release

The tag version is used to set the version during the build, the value in `pyproject.toml` is not expected to be kept up-to-date.
