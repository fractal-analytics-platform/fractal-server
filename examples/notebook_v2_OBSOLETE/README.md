1. Clone the appropriate branch of the repo
```
git clone git@github.com:fractal-analytics-platform/fractal-server.git -b dev-v2
```

2. Move to the appropriate folder
```
cd fractal-server/notebook_v2
```

3. Create and activate new venv
```
python -m venv venv
source venv/bin/activate
```

4. Install some dependencies
```
pip install jupyter devtools
```

5. Install current `fractal-server`:
```
pip install -e ..
```

6. Run jupyter
```
jupyter notebook
```
and select one of the notebooks.
