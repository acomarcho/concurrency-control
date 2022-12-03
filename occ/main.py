from input_parser import InputParser

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
  print(input_parser.transactions)
  print(input_parser.resources)
  print(input_parser.schedule)

  # TODO: Add functionality biar bisa return urutan eksekusi yang sesuai, contoh XL1(X); R1(X); XL2(Y); R2(Y); C2; XL1(Y); R1(Y); C1