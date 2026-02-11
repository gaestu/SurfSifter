# Vendored Dependencies

This directory contains pre-built wheels for dependencies that require special handling.

## pytsk3

**Version:** 20231007  
**Reason:** pytsk3 requires compilation against sleuthkit libraries. Pre-building ensures:
- Consistent builds across environments
- Faster installation
- Works on systems without build tools

### Rebuilding pytsk3 wheel

If you need to rebuild for a different Python version or platform:

```bash
podman run --rm -it \
  -v "$PWD:/work:Z" \
  quay.io/pypa/manylinux2014_x86_64 \
  bash -lc '
    /opt/python/cp311-cp311/bin/python -m pip install -U pip wheel auditwheel &&
    yum -y install gcc gcc-c++ make autoconf automake libtool &&
    /opt/python/cp311-cp311/bin/pip wheel --no-binary :all: -w /work/vendor/wheels pytsk3==20231007 &&
    auditwheel repair /work/vendor/wheels/pytsk3-*.whl -w /work/vendor/wheels &&
    rm /work/vendor/wheels/pytsk3-*-linux_*.whl
  '
```

For other Python versions, replace `cp311-cp311` with the appropriate version (e.g., `cp312-cp312` for Python 3.12).