import logging
import os.path
import progressbar
import sqlite3


class LibraryHelper():
    """A helper class for handling iPhoto DB."""

    def __init__(self, iphoto_path):
        """
        Construct the Helper instance.

        :param iphoto_path: Path for the iPhoto library.
        """
        self.iphoto_path = iphoto_path
        if not os.path.exists(self.iphoto_path):
            raise Exception('iPhoto not found' + self.iphoto_path)

        self.lib_db_path = os.path.join(iphoto_path, 'Database',
                                        'Library.apdb')

        if not os.path.exists(self.lib_db_path):
            raise Exception('Library.apdb not found')

        self.conn = None

    def connect(self):
        """Connect to the Library.apdb that is the main part of iPhoto DBs."""
        if self.conn is None:
            self.conn = sqlite3.connect(self.lib_db_path,
                                        isolation_level='EXCLUSIVE')

    def close(self):
        """Close the connection."""
        if self.conn is not None:
            self.conn.close()
            self.conn = None


class FlaggingHelper(LibraryHelper):
    """A helper class for hadling keywords."""

    def add(self, path, keyword_id):
        """
        Adds a keyword to an image has the specified path.

        :param path: Path for the image to add a keyword.
        :param keyword_id: ID number of the keyword. You can see the ID by
                           sqplite3.
        """
        self.add_all((path,), keyword_id)

    def add_all(self, paths, keyword_id):
        """
        Adds a keyword to all images has specified paths.

        :param paths: Path for the image to add a keyword.
        :param keyword_id: ID number of the keyword. You can see the ID by
                           sqplite3.
        """
        self.connect()

        def param_generator():
            """
            A generator for SQL parameters for adding a keyword to images.

            :returns: A tuple of ``(RKVersion.modelId,)``.
            """
            cur = self.conn.cursor()
            cur.execute('SELECT max(modelId) FROM RKFlaggingForVersion;')

            q = """
                SELECT versionId
                FROM TempDupmark_1
                    WHERE imagePath = ?;
                """

            for path in paths:
                cur.execute(q, (path,))

                res = cur.fetchone()
                if res is not None:
                    version_id = res[0]
                    yield (version_id,)
                    self.update()

        try:
            cur = self.conn.cursor()
            self.preadd()

            # Use 2 temporary tables, these make greater performance.
            # At first, we used INNER JOIN but it was very very slow.
            # So, we decided to use 2 temporary tables.
            # TempDupmark_0 has 2 columns (versionNumber, masterId) and the
            # records based on the records that have a greater versionNumber
            # (means newer). TempDuomark_1 has 2 columns (versionId, imagePath)
            # and the records based on the records that have a greater
            # versionNumber.
            q = """
                CREATE TEMP TABLE TempDupmark_0
                AS SELECT
                    MAX(versionNumber) versionNumber,
                    masterId
                FROM RKVersion
                GROUP BY masterId;

                CREATE UNIQUE INDEX TempDupmark_0_index
                ON TempDupmark_0(masterId);

                CREATE TEMPORARY TABLE TempDupmark_1
                AS SELECT
                    RKVersion.modelId versionId,
                    RKMaster.imagePath imagePath
                FROM RKVersion
                INNER JOIN TempDupmark_0
                    ON RKMaster.modelId = TempDupmark_0.masterId
                    AND RKVersion.versionNumber = TempDupmark_0.versionNumber
                INNER JOIN RKMaster
                    ON RKVersion.masterId = RKMaster.modelId;

                CREATE UNIQUE INDEX TempDupmark_1_index
                ON TempDupmark_1(imagePath);

                DROP TABLE TempDupmark_0;
                """
            cur.execute(q)

            # Unflag all
            cur.execute('UPDATE RKVersion SET isFlagged = 0;')

            q = """
                UPDATE RKVersion SET isFlagged = 1 WHERE modelId = ?;
                """
            cur.executemany(q, param_generator())

            cur.execute('DROP TABLE TempDupmark_1;')
            self.conn.commit()
            self.postadd()
        finally:
            self.close()

    def preadd(self):
        """
        Handles before adding all keyword. Overriding this method is useful for
        displaying the progress.
        """
        logging.info('Applying to iPhoto...')

    def update(self):
        """
        Handles after each adding a keyword. Overriding this method is useful
        for displaying the progress.
        """
        pass

    def postadd(self):
        """
        Handles after all keyword were added. Overriding this method is useful
        for displaying the progress.
        """
        logging.info('Done.')


class FlaggingHelperWithProgressbar(FlaggingHelper):
    def __init__(self, iphoto_path):
        """A helper class for hadling keywords with a progress bar."""
        super(FlaggingHelperWithProgressbar, self).__init__(iphoto_path)
        self.pbar = None

    def add_all(self, paths, keyword_id):
        """
        Adds a keyword to all images has specified paths.

        :param paths: Path for the image to add a keyword.
        :param keyword_id: ID number of the keyword. You can see the ID by
                           sqplite3.
        """
        self._image_paths = paths
        super(FlaggingHelperWithProgressbar, self).add_all(paths, keyword_id)
        self._image_paths = None

    def preadd(self):
        """Prepares a progress bar."""
        logging.debug('Applying to iPhoto...')
        path_count = len(self._image_paths)
        pbar_options = {
            'widgets': ['Adding keyword: ',
                        progressbar.Counter(),
                        ' / {0} '.format(path_count),
                        progressbar.Bar()],
            'maxval': path_count
        }
        self.pbar = progressbar.ProgressBar(**pbar_options)
        self.pbar.start()

    def update(self):
        """Updates the progress bar."""
        self.pbar.update(self.pbar.currval + 1)

    def postadd(self):
        """Finishs the progress bar."""
        logging.debug('Done.')
        self.pbar.finish()


class RenamingHelper(LibraryHelper):
    def rename_all(self, paths, renamer):
        """
        Add a name rename to the specified images.

        :param paths: A iterable collection has paths.
        :param renamer: Rename function that take 2 arguments (old name, the
                        path). And it should returns new name string.
        """
        self.connect()

        def param_generator():
            """
            A generator for SQL parameters for renaming images.

            :returns: A tuple of ``(new_name, RKVersion.modelId)``.
            """
            cur = self.conn.cursor()
            params = []

            for path in paths:
                q = """
                    SELECT
                        name,
                        versionId
                    FROM TempDupmark_1
                    WHERE imagePath = ?;
                    """
                cur.execute(q, (path,))

                res = cur.fetchone()
                if res is not None:
                    yield params.append((renamer(res[0], path), res[1]))
                    self.update()

        try:
            self.prerename()

            cur = self.conn.cursor()

            # Use 2 temporary tables, these make greater performance.
            # At first, we used INNER JOIN but it was very very slow.
            # So, we decided to use 2 temporary tables.
            # TempDupmark_0 has 2 columns (versionNumber, masterId) and the
            # records based on the records that have a greater versionNumber
            # (means newer). TempDuomark_1 has 3 columns (name, versionId,
            # imagePath) and the records based on the records that have a
            # greater versionNumber.
            q = """
                CREATE TEMP TABLE TempDupmark_0
                AS SELECT
                    MAX(versionNumber) versionNumber,
                    masterId
                FROM RKVersion
                GROUP BY masterId;

                CREATE UNIQUE INDEX TempDupmark_0_index
                ON TempDupmark_0(masterId);

                CREATE TEMPORARY TABLE TempDupmark_1
                AS SELECT
                    RKVersion.name name,
                    RKVersion.modelId versionId,
                    RKMaster.imagePath imagePath
                FROM RKVersion
                INNER JOIN TempDupmark_0
                    ON RKMaster.modelId = TempDupmark_0.masterId
                    AND RKVersion.versionNumber = TempDupmark_0.versionNumber
                INNER JOIN RKMaster
                    ON RKVersion.masterId = RKMaster.modelId;

                CREATE UNIQUE INDEX TempDupmark_1_index
                ON TempDupmark_1(imagePath);

                DROP TABLE TempDupmark_0;
                """
            cur.executescript(q)

            q = 'UPDATE RKVersion SET name = ? WHERE modelId = ?;'
            cur.executemany(q, param_generator())

            cur.execute('DROP TABLE TempDupmark_1;')
            self.conn.commit()

            self.postrename()
        finally:
            self.close()

    def prerename():
        logging.info('Renaming...')

    def update():
        pass

    def postrename():
        logging.info('Done.')
        pass


class RenamingHelperWithProgressbar(RenamingHelper):
    """A helper class for renaming with a progress bar."""

    def __init__(self, iphoto_path):
        super(RenamingHelperWithProgressbar, self).__init__(iphoto_path)
        self.pbar = None

    def rename_all(self, paths, renamer):
        self._image_paths = paths
        super(RenamingHelperWithProgressbar, self).rename_all(paths, renamer)
        self._image_paths = None

    def prerename(self):
        """Prepares a progress bar."""
        logging.debug('Renaming...')
        path_count = len(self._image_paths)
        pbar_options = {
            'widgets': ['Renaming: ',
                        progressbar.Counter(),
                        ' / {0} '.format(path_count),
                        progressbar.Bar()],
            'maxval': path_count
        }
        self.pbar = progressbar.ProgressBar(**pbar_options)
        self.pbar.start()

    def update(self):
        """Updates the progress bar."""
        self.pbar.update(self.pbar.currval + 1)

    def postrename(self):
        """Finishs the progress bar."""
        self.pbar.finish()
        logging.debug('Done.')
