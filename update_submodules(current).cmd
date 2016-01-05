@echo off
for /f %%i in ('git rev-parse --abbrev-ref HEAD') do set current_branch=%%i
echo pull changes for all submodules from branch: %current_branch%
git submodule foreach git pull origin %current_branch%
