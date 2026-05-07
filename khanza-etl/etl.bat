@echo off
cd /d E:\Downloads\visuadata\khanza-etl
python etl.py
echo Menutup dalam 10 detik...
timeout /t 10 >nul
exit
