@echo off
echo pull changes for all submodules from branch: master
git submodule foreach git pull origin master
