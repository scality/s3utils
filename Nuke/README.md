# s3utils Nuke.js
Eliminates all versions except the most recent versions.

## How it works
* If deleteLatest is true, all versions including the latest will be deleted.
* Otherwise, the script loops through every version in the array. If the version is not the latest, it is put in a new array. Once every element has been checked, all the keys within that array will be deleted meaning the only element in the bucket is the current version.

## Usage
`npm install ...'

### nuke.js
```js
node nuke.js
```
