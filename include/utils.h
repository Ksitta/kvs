#pragma once

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cstring>
#include <sstream>
#include <vector>

namespace utils
{

static inline bool exist(std::string path)
{
    return (::access(path.c_str(), 0) == 0);
}

/**
 * Check whether directory exists
 * @param path directory to be checked.
 * @return ture if directory exists, false otherwise.
 */
static inline bool dirExists(std::string path)
{
    struct stat st;
    int ret = stat(path.c_str(), &st);
    return ret == 0 && st.st_mode & S_IFDIR;
}

/**
 * list all filename in a directory
 * @param path directory path.
 * @param ret all files name in directory.
 * @return files number.
 */
static inline int scanDir(std::string path, std::vector<std::string> &ret)
{
    DIR *dir;
    struct dirent *rent;
    dir = opendir(path.c_str());
    if (dir == nullptr)
    {
        return -1;
    }
    char s[100];
    while ((rent = readdir(dir)))
    {
        strcpy(s, rent->d_name);
        if (s[0] != '.')
        {
            ret.push_back(s);
        }
    }
    closedir(dir);
    return ret.size();
}

/**
 * Create directory
 * @param path directory to be created.
 * @return 0 if directory is created successfully, -1 otherwise.
 */
static inline int _mkdir(const char *path)
{
    return ::mkdir(path, 0775);
}

/**
 * Create directory recursively
 * @param path directory to be created.
 * @return 0 if directory is created successfully, -1 otherwise.
 */
static inline int mkdir(const char *path)
{
    std::string currentPath = "";
    std::string dirName;
    std::stringstream ss(path);

    while (std::getline(ss, dirName, '/'))
    {
        currentPath += dirName;
        if (!dirExists(currentPath) && _mkdir(currentPath.c_str()) != 0)
        {
            return -1;
        }
        currentPath += "/";
    }
    return 0;
}

/**
 * Delete a empty directory
 * @param path directory to be deleted.
 * @return 0 if delete successfully, -1 otherwise.
 */
static inline int rmdir(const char *path)
{
    return ::rmdir(path);
}

/**
 * Delete a file
 * @param path file to be deleted.
 * @return 0 if delete successfully, -1 otherwise.
 */
static inline int rmfile(const char *path)
{
    return ::unlink(path);
}

}  // namespace utils