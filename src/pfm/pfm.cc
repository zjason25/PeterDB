#include "src/include/pfm.h"

#include <cstdio>
#include <cstdlib>
#include <unistd.h>
#include <cstring>

namespace PeterDB {
    PagedFileManager &PagedFileManager::instance() {
        static PagedFileManager _pf_manager = PagedFileManager();
        return _pf_manager;
    }

    PagedFileManager::PagedFileManager() = default;

    PagedFileManager::~PagedFileManager() = default;

    PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

    PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

    RC PagedFileManager::createFile(const std::string &fileName) {
        const char* fileName_c = fileName.c_str();
        //if fileName does not already exist
        if (access(fileName_c, F_OK) != 0) {
            FILE * pageFile = fopen(fileName_c, "wb+");
            //create a hidden page in the file here
            FileHandle fileHandle;
            fileHandle.openedFile = pageFile;
            fileHandle.createHiddenPage();
            fclose(pageFile);
            return 0;
        }
        return -1;
    }

    RC PagedFileManager::destroyFile(const std::string &fileName) {
        const char* fileName_c = fileName.c_str();
        if (access(fileName_c, F_OK) == 0) {
            if (remove(fileName_c) == 0) {return 0;}
        }
        // fileHandle does not exist
        return -1;
    }

    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        const char* fileName_c = fileName.c_str();
        if (access(fileName_c, F_OK) == 0) {
            FILE* pageFile = fopen(fileName_c, "rb+");
            fileHandle.updateOpenedFile(pageFile);
            return 0;
        }
        // file does not exist
        return -1;
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        if (fileHandle.openedFile != nullptr) {
            fclose(fileHandle.openedFile);
            fileHandle.openedFile = nullptr;
            return 0;
        }
        // fileHandle does not exist
        return -1;

    }

    FileHandle::FileHandle() {
        openedFile = nullptr; // FILE * . default a nullptr
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
    }

    FileHandle::~FileHandle() = default;

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        PageNum totalPages = getNumberOfPages();
        if (pageNum <= totalPages) {
            fseek(openedFile, pageNum * PAGE_SIZE + PAGE_SIZE, SEEK_SET); //skip the hidden page
            fread(data, sizeof(char), PAGE_SIZE, openedFile);
            //update readPageCounter
            readPageCounter = getReadPageCnt();
            readPageCounter += 1;
            writeReadPageCnt(readPageCounter);
            return 0;
        }
        perror("Page does not exist!");
        return -1;
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {
        PageNum totalPages = getNumberOfPages();
        if (totalPages == 0 && pageNum == 0) {
            appendPage(data);
        }
        else if (pageNum <= totalPages) {
            fseek(openedFile, sizeof(char) * PAGE_SIZE * pageNum + PAGE_SIZE, SEEK_SET);
            fwrite(data, sizeof(char), PAGE_SIZE, openedFile);
            fflush(openedFile);
            //update writePageCounter
            writePageCounter = getWritePageCnt();
            writePageCounter += 1;
            writeWritePageCnt(writePageCounter);
            return 0;
        }
        perror("Page does not exist!");
        return -1;
    }

    RC FileHandle::appendPage(const void *data) {
        fseek(openedFile, 0, SEEK_END);
        fwrite(data, sizeof(char), PAGE_SIZE, openedFile);
        fflush(openedFile);
        //update appendPageCounter
        appendPageCounter = getAppendPageCnt();
        appendPageCounter += 1;
        writeAppendPageCnt(appendPageCounter);
        //update the number of pages
        unsigned pageNumber = getNumberOfPages();
        pageNumber++;
        writeNumberOfPages(pageNumber);
        return 0;
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        readPageCount = readPageCounter;
        writePageCount = writePageCounter;
        appendPageCount = appendPageCounter;
        return 0;
    }

    void FileHandle::updateOpenedFile(FILE * pageFile) {
        openedFile = pageFile;
        readPageCounter = getReadPageCnt();
        writePageCounter = getWritePageCnt();
        appendPageCounter = getAppendPageCnt();
    }

    unsigned FileHandle::getNumberOfPages() {
        unsigned numPages = 0;
        fseek(openedFile, 0, SEEK_SET);
        fread(&numPages, sizeof(unsigned), 1, openedFile);
        return numPages;
    }

    unsigned FileHandle::getReadPageCnt() {
        unsigned readPageCnt = 0;
        fseek(openedFile, sizeof(unsigned), SEEK_SET);
        fread(&readPageCnt, sizeof(unsigned), 1, openedFile);
        return readPageCnt;
    }

    unsigned FileHandle::getWritePageCnt() {
        unsigned writePageCnt = 0;
        fseek(openedFile, sizeof(unsigned)*2, SEEK_SET);
        fread(&writePageCnt, sizeof(unsigned), 1, openedFile);
        return writePageCnt;
    }

    unsigned FileHandle::getAppendPageCnt() {
        fseek(openedFile, sizeof(unsigned)*3, SEEK_SET);
        unsigned appendPageCnt = 0;
        fread(&appendPageCnt, sizeof(unsigned), 1, openedFile);
        return appendPageCnt;
    }

    RC FileHandle::writeNumberOfPages(unsigned numOfPages) {
        fseek(openedFile, 0, SEEK_SET);
        fwrite(&numOfPages, sizeof(unsigned), 1, openedFile);
        fflush(openedFile);
        return 0;
    }

    RC FileHandle::writeReadPageCnt(unsigned readPageCnt) {
        fseek(openedFile, sizeof(unsigned), SEEK_SET);
        fwrite(&readPageCnt, sizeof(unsigned), 1, openedFile);
        fflush(openedFile);
        return 0;
    }

    RC FileHandle::writeWritePageCnt(unsigned writePageCnt) {
        fseek(openedFile, sizeof(unsigned)*2, SEEK_SET);
        fwrite(&writePageCnt, sizeof(unsigned), 1, openedFile);
        fflush(openedFile);
        return 0;
    }

    RC FileHandle::writeAppendPageCnt(unsigned appendPageCnt) {
        fseek(openedFile, sizeof(unsigned)*3, SEEK_SET);
        fwrite(&appendPageCnt, sizeof(unsigned), 1, openedFile);
        fflush(openedFile);
        return 0;
    }

    void FileHandle::createHiddenPage() {
        void *data = malloc(PAGE_SIZE);
        unsigned numPages = 0, readPageCnt = 0, writePageCnt = 0, appendPageCnt = 0;
        unsigned storage_size = sizeof (unsigned);
        char* bytePtr = static_cast<char*>(data);

        memcpy(bytePtr,&numPages,storage_size);
        bytePtr+storage_size;
        memcpy(bytePtr,&readPageCnt,storage_size);
        bytePtr+storage_size;
        memcpy(bytePtr,&writePageCnt,storage_size);
        bytePtr+storage_size;
        memcpy(bytePtr,&appendPageCnt,storage_size);

        //write the hidden page
        fwrite(data, sizeof(char), PAGE_SIZE, openedFile);
        fflush(openedFile);
        free(data);
    }

} // namespace PeterDB