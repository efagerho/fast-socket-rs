# Code Examples

This chapter collects small programs that exercise the public socket APIs
directly.

The examples focus on the backend-neutral contracts first. Backend construction
belongs in `main`, while the packet loop itself should usually be generic over
the core traits.
