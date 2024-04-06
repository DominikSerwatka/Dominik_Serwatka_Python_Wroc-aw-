import sys
import csv


def copy_csv(input_path, output_path):
    with open(input_path, mode='r', newline='') as input_file:
        with open(output_path, mode='w', newline='') as output_file:
            reader = csv.reader(input_file)
            input_list = [[row[0], row[1], row[2]] for row in reader]
            writer = csv.writer(output_file)
            name_set = {row[0] for row in input_list}
            input_dict = {item: {} for item in name_set}

            for key in input_dict.keys():
                input_dict[key] = {item: [] for item in name_set if key != item}

            for row in input_list:
                if not input_dict[row[0]][row[1]]:
                    input_dict[row[0]][row[1]].append(int(row[2]))
                else:
                    input_dict[row[0]][row[1]][0] += int(row[2])

            saldo_dict = {item: 0 for item in name_set}
            saldo = 0
            for item in name_set:
                for key, value in input_dict[item].items():
                    saldo += sum(value)
                    saldo_dict[key] -= sum(value)
                saldo_dict[item] += saldo
                saldo = 0

            saldo_plus = {}
            saldo_minus = {}

            for key, value in saldo_dict.items():
                if value < 0:
                    saldo_minus[key] = value
                else:
                    saldo_plus[key] = value

            optimized_transactions = []

            for debtor, debt_amount in saldo_minus.items():
                debt_amount = -debt_amount
                while debt_amount > 0:
                    for creditor, credit_amount in list(saldo_plus.items()):
                        if credit_amount > 0:
                            payment = min(debt_amount, credit_amount)
                            optimized_transactions.append((debtor, creditor, payment))
                            saldo_plus[creditor] -= payment
                            debt_amount -= payment
                            if saldo_plus[creditor] == 0:
                                del saldo_plus[creditor]
                            if debt_amount == 0:
                                break
            for item in optimized_transactions:
                writer.writerow(item)


if __name__ == '__main__':
    # Check if pass correct number of arguments
    if len(sys.argv) != 3:
        print("Have to be: python main.py <path to input file> <path to output file>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    try:
        copy_csv(input_path, output_path)
    except Exception as e:
        print(f"Error: {e}")
