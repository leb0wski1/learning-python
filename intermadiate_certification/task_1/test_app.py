import unittest
from app import phone_number_detect_with_file_read

class TestCalculations(unittest.TestCase):

    def test_positive(self):
        """Testing phone_number_detect_with_file_read function."""
        self.assertTrue(phone_number_detect_with_file_read('numbers.txt'))
                
    @unittest.expectedFailure
    def test_negative(self):
        """Testing phone_number_detect_with_file_read failure."""
        phone_number_detect_with_file_read('Not existed path to file!')
             
if __name__ == '__main__':
    unittest.main()

# for-check