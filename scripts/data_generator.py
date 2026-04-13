"""
ETL-SYNC Data Generator
========================
Simulates realistic e-commerce traffic by generating INSERT/UPDATE/DELETE
operations on MySQL to test the CDC pipeline under load.

Usage:
    python data_generator.py [--rate RATE] [--duration DURATION] [--host HOST] [--port PORT]

Examples:
    python data_generator.py --rate 5 --duration 60
    python data_generator.py --rate 10 --duration 120 --host localhost --port 3307
"""

import argparse
import random
import time
import sys
import signal
from datetime import datetime

import mysql.connector

# ── Configuration ──────────────────────────────────────────────
CUSTOMERS = [
    ("Nguyen Minh Tuan",  "tuannm@gmail.com",    "101 Nguyen Du, Q1, HCM"),
    ("Le Hoang Nam",      "namle@gmail.com",      "55 Le Lai, Q1, HCM"),
    ("Pham Thu Ha",       "hapham@yahoo.com",     "78 Pasteur, Q3, HCM"),
    ("Tran Quoc Bao",     "baotran@outlook.com",  "22 Hai Ba Trung, Q1, HCM"),
    ("Vo Hong Anh",       "anhvo@gmail.com",      "33 Nam Ky Khoi Nghia, Q3, HCM"),
    ("Dang Thanh Long",   "longdt@gmail.com",     "44 Nguyen Thi Minh Khai, Q3, HCM"),
    ("Bui Van Duc",       "ducbui@gmail.com",     "66 Cong Hoa, Tan Binh, HCM"),
    ("Ngo Phuong Linh",   "linhngo@gmail.com",    "12 Truong Son, Tan Binh, HCM"),
    ("Ly Quang Huy",      "huyly@gmail.com",      "88 Hoang Van Thu, Phu Nhuan, HCM"),
    ("Mai Xuan Truong",   "truongmx@gmail.com",   "99 Phan Dang Luu, Binh Thanh, HCM"),
]

ORDER_TRANSITIONS = {
    "pending":    "confirmed",
    "confirmed":  "processing",
    "processing": "shipped",
    "shipped":    "delivered",
}

# ── Globals ────────────────────────────────────────────────────
running = True
stats = {"insert": 0, "update": 0, "delete": 0, "error": 0}


def signal_handler(sig, frame):
    global running
    running = False
    print("\n⏹  Stopping generator...")


def get_connection(host, port, user, password, database):
    return mysql.connector.connect(
        host=host, port=port, user=user, password=password,
        database=database, autocommit=True,
    )


def create_order(cursor):
    """Create a new order with 1-3 random items."""
    customer = random.choice(CUSTOMERS)

    # Get available product IDs
    cursor.execute("SELECT id, price FROM products WHERE status = 'active'")
    products = cursor.fetchall()
    if not products:
        return

    # Pick 1-3 random products
    num_items = random.randint(1, 3)
    selected = random.sample(products, min(num_items, len(products)))

    # Calculate total
    items = []
    total = 0.0
    for prod_id, price in selected:
        qty = random.randint(1, 3)
        items.append((prod_id, qty, float(price)))
        total += float(price) * qty

    # Insert order
    cursor.execute(
        """INSERT INTO orders (customer_name, customer_email, status, total_amount, shipping_address)
           VALUES (%s, %s, 'pending', %s, %s)""",
        (customer[0], customer[1], round(total, 2), customer[2])
    )
    order_id = cursor.lastrowid

    # Insert order items
    for prod_id, qty, price in items:
        cursor.execute(
            """INSERT INTO order_items (order_id, product_id, quantity, unit_price)
               VALUES (%s, %s, %s, %s)""",
            (order_id, prod_id, qty, price)
        )

    # Update inventory
    for prod_id, qty, _ in items:
        cursor.execute(
            """UPDATE inventory SET reserved = reserved + %s, updated_at = NOW()
               WHERE product_id = %s AND warehouse = 'main'""",
            (qty, prod_id)
        )

    stats["insert"] += 1
    print(f"  ✅ INSERT order #{order_id} | {len(items)} items | ${total:.2f}")


def update_order_status(cursor):
    """Advance a random order to the next status."""
    statuses = list(ORDER_TRANSITIONS.keys())
    status = random.choice(statuses)

    cursor.execute(
        "SELECT id, status FROM orders WHERE status = %s ORDER BY RAND() LIMIT 1",
        (status,)
    )
    row = cursor.fetchone()
    if not row:
        return

    order_id, current_status = row
    next_status = ORDER_TRANSITIONS[current_status]

    cursor.execute(
        "UPDATE orders SET status = %s, updated_at = NOW() WHERE id = %s",
        (next_status, order_id)
    )

    # If shipped → delivered, release reserved inventory
    if next_status == "delivered":
        cursor.execute(
            """UPDATE inventory i
               JOIN order_items oi ON oi.product_id = i.product_id
               SET i.quantity = i.quantity - oi.quantity,
                   i.reserved = i.reserved - oi.quantity,
                   i.updated_at = NOW()
               WHERE oi.order_id = %s AND i.warehouse = 'main'""",
            (order_id,)
        )

    stats["update"] += 1
    print(f"  🔄 UPDATE order #{order_id}: {current_status} → {next_status}")


def update_inventory(cursor):
    """Simulate a random inventory restock."""
    cursor.execute("SELECT id, product_id, quantity FROM inventory ORDER BY RAND() LIMIT 1")
    row = cursor.fetchone()
    if not row:
        return

    inv_id, prod_id, qty = row
    restock = random.randint(10, 100)

    cursor.execute(
        "UPDATE inventory SET quantity = quantity + %s, updated_at = NOW() WHERE id = %s",
        (restock, inv_id)
    )

    stats["update"] += 1
    print(f"  📦 RESTOCK inventory #{inv_id} (product #{prod_id}): +{restock} units")


def cancel_order(cursor):
    """Cancel a pending/confirmed order and release reserved inventory."""
    cursor.execute(
        "SELECT id FROM orders WHERE status IN ('pending','confirmed') ORDER BY RAND() LIMIT 1"
    )
    row = cursor.fetchone()
    if not row:
        return

    order_id = row[0]

    # Release reserved inventory
    cursor.execute(
        """UPDATE inventory i
           JOIN order_items oi ON oi.product_id = i.product_id
           SET i.reserved = GREATEST(0, i.reserved - oi.quantity),
               i.updated_at = NOW()
           WHERE oi.order_id = %s AND i.warehouse = 'main'""",
        (order_id,)
    )

    cursor.execute(
        "UPDATE orders SET status = 'cancelled', updated_at = NOW() WHERE id = %s",
        (order_id,)
    )

    stats["update"] += 1
    print(f"  ❌ CANCEL order #{order_id}")


def update_product_price(cursor):
    """Simulate a price change on a random product."""
    cursor.execute("SELECT id, name, price FROM products WHERE status = 'active' ORDER BY RAND() LIMIT 1")
    row = cursor.fetchone()
    if not row:
        return

    prod_id, name, old_price = row
    # ±5-20% price change
    factor = random.uniform(0.8, 1.2)
    new_price = round(float(old_price) * factor, 2)

    cursor.execute(
        "UPDATE products SET price = %s, updated_at = NOW() WHERE id = %s",
        (new_price, prod_id)
    )

    stats["update"] += 1
    direction = "📈" if new_price > float(old_price) else "📉"
    print(f"  {direction} PRICE product #{prod_id} ({name}): ${float(old_price):.2f} → ${new_price:.2f}")


# ── Operation weights ──────────────────────────────────────────
OPERATIONS = [
    (create_order,         40),   # 40% - create new orders
    (update_order_status,  25),   # 25% - advance order status
    (update_inventory,     15),   # 15% - restock inventory
    (cancel_order,         10),   # 10% - cancel orders
    (update_product_price, 10),   # 10% - change product prices
]


def pick_operation():
    total = sum(w for _, w in OPERATIONS)
    r = random.randint(1, total)
    cumulative = 0
    for op, weight in OPERATIONS:
        cumulative += weight
        if r <= cumulative:
            return op
    return OPERATIONS[0][0]


def main():
    global running
    signal.signal(signal.SIGINT, signal_handler)

    parser = argparse.ArgumentParser(description="ETL-SYNC Data Generator")
    parser.add_argument("--host",     default="localhost", help="MySQL host")
    parser.add_argument("--port",     default=3307, type=int, help="MySQL port")
    parser.add_argument("--user",     default="root",     help="MySQL user")
    parser.add_argument("--password", default="rootpwd",  help="MySQL password")
    parser.add_argument("--database", default="etl_db",   help="MySQL database")
    parser.add_argument("--rate",     default=2,   type=float, help="Operations per second")
    parser.add_argument("--duration", default=0,   type=int,   help="Duration in seconds (0=infinite)")
    args = parser.parse_args()

    interval = 1.0 / args.rate
    start_time = time.time()

    print("=" * 60)
    print(f"🚀 ETL-SYNC Data Generator")
    print(f"   Host: {args.host}:{args.port}")
    print(f"   Rate: {args.rate} ops/sec | Duration: {'infinite' if args.duration == 0 else f'{args.duration}s'}")
    print("=" * 60)

    conn = get_connection(args.host, args.port, args.user, args.password, args.database)
    cursor = conn.cursor()

    ops_count = 0
    try:
        while running:
            if args.duration > 0 and (time.time() - start_time) >= args.duration:
                break

            op = pick_operation()
            try:
                op(cursor)
                ops_count += 1
            except Exception as e:
                stats["error"] += 1
                print(f"  ⚠️  ERROR: {e}")

            # Jitter: ±30% of interval
            jitter = interval * random.uniform(0.7, 1.3)
            time.sleep(jitter)

    finally:
        cursor.close()
        conn.close()

        elapsed = time.time() - start_time
        print("\n" + "=" * 60)
        print(f"📊 Generator Summary")
        print(f"   Duration:  {elapsed:.1f}s")
        print(f"   Total ops: {ops_count}")
        print(f"   Inserts:   {stats['insert']}")
        print(f"   Updates:   {stats['update']}")
        print(f"   Errors:    {stats['error']}")
        print(f"   Avg rate:  {ops_count / max(elapsed, 1):.1f} ops/sec")
        print("=" * 60)


if __name__ == "__main__":
    main()
