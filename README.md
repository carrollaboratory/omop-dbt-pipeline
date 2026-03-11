# omop-dbt-pipeline




### Getting Set Up

1. Start a new Jupyter environment with the startup script [link tbd]

2. Place files in home/jupyter/ 
    - ssh key (follow this link for how to [generate an ssh key from GitHub](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent))
    - harmonization_setup.py
    - ssh_startup.py
    
3. Run `~/.git_config` in the `home/jupyter` folder to make sure the config file is empty. If the `.git_config` file is not empty, you can delete the contents because next steps will fill it out.

4. Run `python ssh_setup.py`

5. Run the following harmonization setup commands (run `python harmonization_setup.py -h to see the explanation of arguments`)
    - `python harmonization_setup.py -s consort_gira -e jupyter -a update_bash_profile`
    - `python harmonization_setup.py -s consort_gira -e jupyter -a update_gitignore`
    - `python harmonization_setup.py -s consort_gira -e jupyter -a clone_pipeline`
    - `python harmonization_setup.py -s consort_gira -e jupyter -a setup_data`

6. Run `source ~/.bash_profile` and then run `activate` to get your virtual environment started.

7. Verify that your ssh key is linked to GitHub by running `ssh -T git@github.com`
    - If the above command does not work, run `eval "$(ssh-agent -s)"` followed by `ssh-add ~/.ssh/id_rsa`
    
8. Go into your pipeline folder with `cd pipeline` and then clone this repository with `git clone git@github.com:carrollaboratory/omop-dbt-pipeline.git`

9. Navigate back into the home/jupyter folder and run `gsutil cp -r gs://fc-secure-1449c349-4ad7-4b81-9a8c-3d9462ab7965/eMERGE_6_Month_Data_External_Release pipeline/_study_data/consort_gira/` and all the data should be in the _study_data folder.

You should be set up and ready to work in dbt!


### dbt Basics

*Note that the omop-dbt-pipeline uses a Duckdb dbt adapter to run so there are times when you may need sql command help and the solution may be specific to Duckdb or the Duckdb dbt adapter.*

dbt has a series of [basic commands](https://docs.getdbt.com/reference/dbt-commands) that will be used variously through this project. In the folder pipeline/omop-dbt-pipeline/run_commands/emerge/consort_gira is a file called **run_eMERGE_6_Month_Data_External_Release_202601**.

In the above run_commands script, you can comment/uncomment lines that you want to run and then in dbt_project/, run the command like `dbt run --select emerge_consort_gira_src_emerge_bmi_ex_release_20260128` to generate the source (src) table. 
