import os

__version__ = "1.2.1"

# Stamped at image build time with the commit being built. The `latest` tag is
# republished on every push while the semantic version rarely moves, so
# without this there is no way to tell which build a running container came
# from — which is exactly the ambiguity that makes a misbehaving deployment
# impossible to tell apart from a bug in the source.
__build__ = os.environ.get("BABEL_BUILD", "").strip() or "source"
__revision__ = f"{__version__}+{__build__}"
