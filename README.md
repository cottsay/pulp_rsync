# pulp_rsync

This package contains a (labotomized) implementation of the rsync wire protocol, which can be used to access content in Pulp.

In its current state, only trivial rsync operations are possible, and the "core" of the rsync algorithm's block transfer is left unimplemented. This means that any files that are requested by the client will be transferred in their entirety.

Pulp distributions are exposed as rsync modules by name. This means that the base path property of pulp distributions is ignored. This is primarily due to rsync's use of module-level authorization.
