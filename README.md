# cleaner
A Go experiment to deal with duplicates

## Usage example
Sample command to find and move duplicates:
```
cleaner -db dropbox.txt -compact -masters "F:\Dropbox\Video" -duplicates "F:\Dropbox\Stuff" -move "F:\Dropbox.removed" -prefix "F:\Dropbox" -apply "F:\Dropbox"
```
Arguments:
* `-db dropbox.txt` - database file path.
* `-compact` - compress database file when changes to files are detected. Default behavior is to append updates.
* `-masters "F:\Dropbox\Video"` - scan all files inside *F:\Dropbox\Video* and find their duplicates. Without this masters (original files) will be searched across all paths in database.
* `-duplicates "F:\Dropbox\Stuff"` - look for duplicate files in *F:\Dropbox\Stuff*. Without this duplicates will be searched across all paths in database. Combined with `-masters` it will find all duplicate videos that are present in *F:\Dropbox\Video* and *F:\Dropbox\Stuff*.
* `-move "F:\Dropbox.removed"` - move found duplicate files to *F:\Dropbox.removed* while preserving their relative path. By default only drive letter is removed, so *F:\Dropbox\Stuff\duplicate* will be moved to *F:\Dropbox.removed\Dropbox\Stuff\duplicate*.
* `-prefix "F:\Dropbox"` - strip *F:\Dropbox* from file paths when moving duplicates. With this option duplicate *F:\Dropbox\Stuff\duplicate* will be moved to *F:\Dropbox.removed\Stuff\duplicate*.
* `-apply` - actually move duplicate files. Without this options intended actions will be printed, but not applied.
* `"F:\Dropbox"` - scan *F:\Dropbox* for changes or new files. Without this option only files that were previously scanned and saved in database would be processed.
