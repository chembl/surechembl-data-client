import unittest

from src.scripts.doc_loader import DocLoader

class DocLoaderTests(unittest.TestCase):

    def setUp(self):
        self.loader = DocLoader()

    def test_create_doc_loader(self):
        self.failUnless( isinstance(self.loader, DocLoader) )

def main():
    unittest.main()

if __name__ == '__main__':
    main()
