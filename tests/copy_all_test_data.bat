cd /d c:\temp\test_data 
@REM fciv.exe -add c:\temp\test_data -wp -sha1 -xml db.xml

xcopy /s C:\temp\test_data X:\

cd /d x:
fciv.exe -v -r -xml db.xml -sha1