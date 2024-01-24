## Project 1 Report


### 1. Basic information
 - Team #: 7
 - Github Repo Link: https://github.com/zhijiz8/cs222-winter24-zhijiz8
 - Student 1 UCI NetID: zhijiz8
 - Student 1 Name: Jason Zheng


### 2. Internal Record Format
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


### 3. Page Format
Every page can store up to 4096 bytes of data defined by `PAGE_SIZE`. Every record is inserted from the left
and grows to the right until there is no more space. The end of every page contains a slot directory with N and F 
-- which stands for number of slots and free space -- staying fixed and occupying 4 bytes each at the end of the page.
The directory grows from the end of the page toward left of the byte array, leaving the middle part of the page as free 
space for new records to be inserted. Each slot in this directory contains a (`offset`, `length`) pair, and each
slot is accessible with the `slotNum` attribute of a record's `RID` as stated above.


### 4. Page Management
Upon creating a file, a hidden page is created but does not count toward the total number of pages. `PageNum`, the 
index of a page, will start from 0. The hidden page contains metadata -- `numPages`, `readPageCnt`, `writePageCnt`, `appendPageCnt`
-- about the page and is accessible through the getter and setter helper functions defined under 
fileHandle class.

When inserting a record, a new page is created to store the record if there is no existing pages at all, incrementing 
page count in the process. Otherwise, the algorithm first look for free space on the last (the most recently appended)
page. If there is no free space, the algorithm will check from Page 0 all the way to the last page again, inserting the
record when free space is found. Lastly, if no free space is found on existing pages, a new page will be created to store
the record.

### 5. Implementation Detail
PagedFileManager utilizes a hidden page and several getters and setters helper functions to access and log operations
performed on files, including read, write, append, as well as the total number of pages.


### 6. Member contribution (for team of two)
- My teammate quit on me on Friday :)


### 7. Other (optional)
I did not include checks for possible failed fwrite, fread, or fflush file related operations since I was able to pass
all the related test cases.
