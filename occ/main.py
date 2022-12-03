from input_parser import InputParser
from transaction_manager import TransactionManager

if __name__ == '__main__':
  print("======================================================")
  print("Serial Optimistic Concurrency Control (OCC)")
  print("======================================================")
  print("Format test case yang bisa diterima:")
  print("R1(X); W2(X); W2(Y); W3(Y); W1(X); C1; C2; C3;")
  print("======================================================")
  test_case = input("Masukkan test case: ")

  input_parser = InputParser()
  input_parser.parse(test_case)
  
  transaction_manager = TransactionManager(input_parser)
  
  transaction_manager.run()