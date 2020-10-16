set folder="D:\_Projetos\VeratyFS\metadata"
cd /d %folder%
del /Q *.*

@REM for /F "delims=" %%i in ('dir /b') do (rmdir "%%i" /s/q || del "%%i" /s/q)