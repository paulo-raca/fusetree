from setuptools import setup

setup(
    name = 'fusetree',
    packages = ['fusetree'],
    version = '0.1.0',
    description = 'Hig-level API for fusepy',
    author = 'Paulo Costa',
    author_email = 'me@paulo.costa.nom.br',
    url = 'https://github.com/paulo-raca/fusetree',
    download_url = 'https://github.com/paulo-raca/fusetree',
    keywords = ['fuse'],
    install_requires = [
        "fusepy",
        "aiohttp"
    ]
)
