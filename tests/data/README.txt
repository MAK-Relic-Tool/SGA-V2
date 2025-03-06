# Invalid .Arciv files
.arciv files present in datasets are NOT VALID

Because .arciv files are built using an absolute path; automated tests on other systems would fail when building

To construct a valid .arciv, the literal '<cwd>' must be replaced with the absolute path to the parent directory of the .arciv file

This will properly assemble all path variables in the .arciv; making it a valid .arciv file

This can be fixed by running the fix_arciv.py script

# Manifest Outdated
Manifests may become outdated if rebuilt from the .arciv files

This can be fixed by running the update_manifest.py script
