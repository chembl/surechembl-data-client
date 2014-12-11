
import os
import re
import logging
import ftplib

logger = logging.getLogger(__name__)

class NewFileReader:
    """
    Provides methods for reading file lists and contents from the SureChEMBL data feed.
    """

    FRONT_FILE_LOC  = "/data/external/frontfile"
    BACK_FILE_LOC   = "/data/external/backfile"
    DAY_FILES_LOC   = FRONT_FILE_LOC + "/{0}/{1}/{2:02d}"
    YEAR_FILES_LOC  = BACK_FILE_LOC + "/{0}"
    NEW_FILES_NAME  = "newfiles.txt"

    SUFFIX_CHEM    = ".chemicals.tsv.gz"
    SUFFIX_BIBLIO  = ".biblio.json.gz"

    SUPP_CHEM_REGEX = r"_supp[0-9]+.chemicals.tsv.gz"
    FILE_PATH_REGEX = r"(.*/)([^/]+$)"

    def __init__(self, ftp):
        """
        Create a NewFileReader object.
        :param ftp: Instance of ftplib.FTP, must be initialized and ready for server interaction.
        """

        self.ftp = ftp
        self.supp_regex = re.compile(self.SUPP_CHEM_REGEX)



    def get_frontfile_new(self, from_date):
        """
        Read a list of new files from the FTP server, for the given date.
        :param from_date: The date to query
        :return: List of absolute file paths on the FTP server.
        :raise ValueError if no data directory exists for the given date
        """

        logger.info( "Identifying new files for {}".format(from_date) )

        new_files_loc = self.DAY_FILES_LOC.format(
            from_date.year,
            from_date.month,
            from_date.day)

        self._change_to_data_dir(new_files_loc)

        data = []
        def handle_binary(more_data):
            data.append(more_data)

        try:
            self.ftp.retrbinary("RETR " + self.NEW_FILES_NAME, handle_binary)
        except ftplib.error_perm, exc:
            if exc.message.startswith("550"):
                raise ValueError("No new files entry was found for [{}]".format(from_date))
            raise

        content = "".join(data)
        rel_file_list = content.split('\n')
        abs_file_list = map( lambda f: "{0}/{1}".format(self.FRONT_FILE_LOC, f), rel_file_list)

        logger.info( "Discovered {} new files".format(len(abs_file_list)) )

        return abs_file_list


    def get_frontfile_all(self, date):
        """
        Read a list of all files from the FTP server, for the given date.
        :param from_date: The date to query
        :return: List of absolute file paths on the FTP server.
        :raise ValueError if no data directory exists for the given date
        """

        logger.info( "Identifying files for day {}".format(date) )

        day_files_path = self.DAY_FILES_LOC.format(
            date.year,
            date.month,
            date.day)

        self._change_to_data_dir(day_files_path)

        ftp_file_list = self.ftp.nlst()
        abs_file_list = map( lambda f: "{0}/{1}".format(day_files_path, f), ftp_file_list)

        logger.info( "Discovered {} files".format(len(abs_file_list)) )

        return abs_file_list

    def get_backfile_year(self, date_obj):
        """
        Read a list of files from the FTP server, for the given backfile year.
        :param date_obj: Date object, only the year is used.
        :return: List of absolute file paths on the FTP server.
        :raise ValueError if no data directory exists for the given date
        """

        year = date_obj.year
        logger.info( "Identifying files for year {}".format(year) )

        year_path = self.YEAR_FILES_LOC.format(year)

        self._change_to_data_dir(year_path)

        ftp_file_list = self.ftp.nlst()
        abs_file_list = map( lambda f: "{0}/{1}".format(year_path, f), ftp_file_list)

        logger.info( "Discovered {} files".format(len(abs_file_list)) )

        return abs_file_list


    def _change_to_data_dir(self, expected_data_dir):
        """A wrapper for changing directory, to raise an appropriate exception when no data found"""
        try:
            self.ftp.cwd(expected_data_dir)
        except ftplib.error_perm, exc:
            if exc.message.startswith("550"):
                raise ValueError("No data found for given date. Target folder: [{}]".format(expected_data_dir))
            else:
                raise


    def select_downloads(self, file_list):
        """
        Select files to download for data processing.
        :param file_list: List of FTP server file paths.
        :return: Filtered list of file paths; only data-feed relevant files will be included.
        """

        logger.info( "Selecting files to download" )

        bibl_files = set()
        chem_files = set()

        for file in file_list:
            if file.endswith(self.SUFFIX_BIBLIO):
                bibl_files.add(file)
            elif file.endswith(self.SUFFIX_CHEM):
                chem_files.add(file)

        supp_chems = filter( lambda f: self.supp_regex.search(f), file_list )

        for sc in supp_chems:
            bibl_files.add( self.supp_regex.sub(self.SUFFIX_BIBLIO, sc) )

        download_list = sorted(bibl_files) + sorted(chem_files)

        logger.info( "Selected {} files for download".format( len(download_list) ) )

        return download_list


    def read_files(self,file_list,target_dir):
        """
        Download the files from the FTP server, into the target folder.
        :param file_list: List of absolute file paths on FTP server. Invalid paths will result in ftplib exceptions
        :param target_dir: Local file path to store the downloads in; will be created if non-existent
        """

        logger.info( "Creating target directory for download: [{}]".format(target_dir) )
        if not os.path.exists(target_dir):
            os.makedirs(target_dir, mode=0755)

        for file_path in file_list:

            matched = re.match(self.FILE_PATH_REGEX, file_path)
            path = matched.group(1)
            file = matched.group(2)

            fhandle = open("{0}/{1}".format(target_dir,file), 'wb')

            logger.info("Changing to remote directory [{}]".format(path))
            logger.info("Downloading [{}]".format(file))

            self.ftp.cwd( path )
            self.ftp.retrbinary("RETR " + file, fhandle.write)

            # TODO implement resilient download / error checking / retry

            fhandle.close()



