## Project 2 Report


### 1. Basic information
 - Team #: 7
 - Github Repo Link: https://github.com/zhijiz8/cs222-winter24-zhijiz8
 - Student 1 UCI NetID: zhijiz8
 - Student 1 Name: Jason Zheng

### 2. Meta-data
Before any tables can be created, `createCatalog()` function must be called to create `Tables` and `Columns`
table to hold information describing existing tables (including `Tables` and `Columns`) and any tables that
will be created.

`Tables` contains four fields:
- table-id: a unique ID describing a table
- table-name: name of the table
- file-name: the name of the file that holds the record for a table (the same as table-name)
- sys: an integer indicating if a table is a system table or not (0 for no, 1 for yes)

`Columns` contains five fields:
- table-id: a unique ID describing a table, the same one as the one in `Tables`
- column-name: name of the attribute
- column-type: type of the attribute, which can be `TypeInt`, `TypeReal`, and `TypeVarchar`
- column-length: `int` describing the length of the attribute 
- column-position: relative position of the attribute


### 3. Internal Record Format (the same as Part 1)
Every record stored in a page starts with a null indicator that contains `ceil(size of the fields / 8)`
number of bytes. Following the null indicator, every non-null Varchar type attribute begins with a length
(4 bytes) followed by the actual characters of variable bytes. Both TypeInt and TypeReal occupy a fixed
size of 4 bytes. Null attributes do not physically occupy any space on the byte array.

A slot directory that contains (`offset`, `length`) pairs is built into the end of every page. Given a record's RID
we can use the `slotNum` attribute to obtain the following:

- page + PAGE_SIZE - 2 * sizeof(unsigned) - rid.slotNum * 2 * sizeof(unsigned): offset
- page + PAGE_SIZE - 2 * sizeof(unsigned) - rid.slotNum * 2 * sizeof(unsigned) + sizeof(unsigned): length

Once (`offset`, `length`) pair is obtained, we can trace to the start of the desired record by adding the offset
to the `page` pointer and  copy `length` amount of bytes in O(1) access time.


### 4. Page Format (the same as Part 1)
Every page can store up to 4096 bytes of data defined by `PAGE_SIZE`. Every record is inserted from the left
and grows to the right until there is no more space. The end of every page contains a slot directory with N and F
-- which stands for number of slots and free space -- staying fixed and occupying 4 bytes each at the end of the page.
The directory grows from the end of the page toward left of the byte array, leaving the middle part of the page as free
space for new records to be inserted. Each slot in this directory contains a (`offset`, `length`) pair, and each
slot is accessible with the `slotNum` attribute of a record's `RID` as stated above.


### 5. Page Management (the same as Part 1)
Upon creating a file, a hidden page is created but does not count toward the total number of pages. `PageNum`, the
index of a page, will start from 0. The hidden page contains metadata -- `numPages`, `readPageCnt`, `writePageCnt`, `appendPageCnt`
-- about the page and is accessible through the getter and setter helper functions defined under
fileHandle class.

When inserting a record, a new page is created to store the record if there is no existing pages at all, incrementing
page count in the process. Otherwise, the algorithm first look for free space on the last (the most recently appended)
page. If there is no free space, the algorithm will check from Page 0 all the way to the last page again, inserting the
record when free space is found. Lastly, if no free space is found on existing pages, a new page will be created to store
the record.


### 6. Describe the following operation logic.
All write operations, `insertTuple`, `deleteTuple`, and `updateTuple` call `getAttribute` using the given tableName, 
retrieve a record descriptor, and pass it to the corresponding `insert`, `delete` and `update` function from the lower level, 
the record based file manager.

#### DeleteRecord

To delete a record, rbfm first locates the record slot with the given RID, go to the record using the `offset` and `length`,
then using `memmove` to shift everything up to but not including the directory left to overwrite the previous position
held by record to be deleted, then finally update slot offsets of all records that shifted left in this operation.
Slot previously held by deleted record is not deleted, but the `length` field is set to 0 since all records must hold at least
1 byte of memory (nullIndicator). Checking for deleted slot and tombstone (to be discussed in update) will precede the delete
operation, which results in returning -1 for RC if `length` of 0 is encountered for deleted slot, or a recursive call to 
`deleteRecord` with the `slotNum` and `pageNum` info found in a tombstone.

#### UpdateRecord
Updating a record involves similar `memmove` operations used in `deleteRecord`, with the additional steps of checking for record
length. Similar to `deleteRecord`, rbfm first locates the record using RID. Then it checks and compare the length of existing
record and the record to be inserted. If the new record is smaller than old record, the new record is memcopied into the same
offset and partially overwrites the content, and an `memmove` operation will shift everything to the right of new record left
by size = len(old_record) - len(new_record) to fill the gap. The slot offset of records that moved are updated accordingly.
In the case that new record is larger than the old record, the old record is first deleted using `deleteRecord`, then the
new record is inserted at the end of the page if there is enough space. If not, an `insertRecord` is called to insert the new 
record in a new page; the resulting `slotNum` and `pageNum` will be incremented by `TOMBSTONE_MARKER`, 4096 bytes, and updated
to the slot they previously occupied in the old page.


### Scan
Calling scan() will not begin the search; it initializes the iterator with states provided in the function arguments that will
later be used by getNextTuple(). A fileHandle object will keep track of the opened page to be scanned. The `conditionAttribute`
will act as the variable to be compared in a SQL `where` clause with the `CompOp` operator for comparison, and `attributeNames`
will be the list of attributes to be projected. `getNextRecord` will repeatedly apply these filter and return record that matches
these filters one by one until `RBF_EOF` is returned. Scanning on deleted records will not succeed 



### 7. Other (optional)
- Freely use this section to tell us about things that are related to the project 1, but not related to the other sections (optional)

- Feedback on the project to help improve the project. (optional)