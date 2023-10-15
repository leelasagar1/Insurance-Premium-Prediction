
from setuptools import find_packages,setup
from typing import List

requirements_file_name = 'requirements.txt'
REMOVE_PACKAGE = '-e .'

def get_requirements()-> List[str]:
    

    with open(requirements_file_name) as file:

        requirement_list = file.readline()

    requirement_list = [name.replace('\n','') for name in requirement_list ]

    if REMOVE_PACKAGE in requirement_list:

        requirement_list.remove(REMOVE_PACKAGE)

    return requirement_list


setup( 
    name="Insurance",
      version='0.0.1',
      description='Insurance premium prediction',
      author = 'Leela Sagar',
      author_email='leelasagar.gudhe@gmail.com',
      packages=find_packages(),
      install_requires = get_requirements()#installs all the required packages
      
      )