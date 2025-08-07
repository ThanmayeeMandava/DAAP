README_Luigi.txt
================


Running the Project:
--------------------
1. **Open Project in Jupyter**:
   - Ensure JupyterLab or Jupyter Notebook is installed on your system.
   - Navigate to the project directory in your terminal.
   - Start JupyterLab (or Notebook):
     ```
     jupyter lab
     ```
   - Open the project file (usually a `.ipynb` file) in Jupyter.

2. **Luigi Dashboard**:
   - Open a terminal in the project directory (or use the Jupyter terminal).
   - Start the Luigi daemon:
     ```
     luigid
     ```
   - Open a web browser and go to `http://localhost:8082` to view the Luigi dashboard and monitor the progress of your pipeline.

3. **Execution of main file**:
   - Run all cells in the notebook by clicking on 'Run All' in the toolbar. This will automatically install all required libraries and execute the project.


Additional Notes:
-----------------
- The Docker containers must be running for the database connections to work.
- The first run of the Jupyter notebook might take longer due to the installation of the libraries.