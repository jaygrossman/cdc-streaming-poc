-- Policy 1: Active policy, 2 coverages, 1 vehicle, 1 driver, 1 claim
INSERT INTO policy (data) VALUES ('{
  "policy_number": "POL-2024-00123",
  "status": "active",
  "effective_date": "2024-01-15",
  "expiration_date": "2025-01-15",
  "policyholder": {
    "first_name": "Jane",
    "last_name": "Doe",
    "date_of_birth": "1985-03-22",
    "contact": {
      "email": "jane.doe@example.com",
      "phone": "+1-555-0142",
      "address": {
        "street": "742 Evergreen Terrace",
        "city": "Springfield",
        "state": "IL",
        "zip": "62704"
      }
    }
  },
  "coverages": [
    {"type": "liability", "limit": 500000, "deductible": 1000, "premium": 1200.00},
    {"type": "collision", "limit": 50000, "deductible": 500, "premium": 800.00}
  ],
  "vehicles": [
    {
      "vin": "1HGBH41JXMN109186",
      "year": 2022,
      "make": "Honda",
      "model": "Civic",
      "drivers": [
        {"name": "Jane Doe", "license_number": "D400-1234-5678", "is_primary": true}
      ]
    }
  ],
  "claims_history": [
    {"claim_id": "CLM-001", "date": "2023-06-15", "amount": 4500.00, "status": "closed", "description": "Rear-end collision"}
  ]
}'::jsonb);

-- Policy 2: Expired policy, 3 coverages, 2 vehicles, 3 drivers, 2 claims
INSERT INTO policy (data) VALUES ('{
  "policy_number": "POL-2024-00456",
  "status": "expired",
  "effective_date": "2023-03-01",
  "expiration_date": "2024-03-01",
  "policyholder": {
    "first_name": "Robert",
    "last_name": "Chen",
    "date_of_birth": "1978-11-08",
    "contact": {
      "email": "r.chen@example.com",
      "phone": "+1-555-0298",
      "address": {
        "street": "1600 Pennsylvania Ave",
        "city": "Austin",
        "state": "TX",
        "zip": "73301"
      }
    }
  },
  "coverages": [
    {"type": "liability", "limit": 1000000, "deductible": 2000, "premium": 1800.00},
    {"type": "collision", "limit": 75000, "deductible": 1000, "premium": 950.00},
    {"type": "comprehensive", "limit": 75000, "deductible": 500, "premium": 600.00}
  ],
  "vehicles": [
    {
      "vin": "5YJSA1DN5DFP14555",
      "year": 2023,
      "make": "Tesla",
      "model": "Model S",
      "drivers": [
        {"name": "Robert Chen", "license_number": "C800-5678-9012", "is_primary": true},
        {"name": "Linda Chen", "license_number": "C800-9876-5432", "is_primary": false}
      ]
    },
    {
      "vin": "WBAPH5C55BA271443",
      "year": 2021,
      "make": "BMW",
      "model": "328i",
      "drivers": [
        {"name": "Robert Chen", "license_number": "C800-5678-9012", "is_primary": true}
      ]
    }
  ],
  "claims_history": [
    {"claim_id": "CLM-002", "date": "2023-09-20", "amount": 12000.00, "status": "closed", "description": "Side impact at intersection"},
    {"claim_id": "CLM-003", "date": "2024-01-05", "amount": 3200.00, "status": "open", "description": "Windshield damage from debris"}
  ]
}'::jsonb);

-- Policy 3: Pending renewal, 1 coverage, 1 vehicle, 2 drivers, no claims
INSERT INTO policy (data) VALUES ('{
  "policy_number": "POL-2024-00789",
  "status": "pending_renewal",
  "effective_date": "2024-06-01",
  "expiration_date": "2025-06-01",
  "policyholder": {
    "first_name": "Maria",
    "last_name": "Garcia",
    "date_of_birth": "1992-07-14",
    "contact": {
      "email": "maria.garcia@example.com",
      "phone": "+1-555-0376",
      "address": {
        "street": "456 Oak Boulevard",
        "city": "Denver",
        "state": "CO",
        "zip": "80202"
      }
    }
  },
  "coverages": [
    {"type": "liability", "limit": 250000, "deductible": 500, "premium": 750.00}
  ],
  "vehicles": [
    {
      "vin": "JM1NDAL70R0100234",
      "year": 2024,
      "make": "Mazda",
      "model": "MX-5 Miata",
      "drivers": [
        {"name": "Maria Garcia", "license_number": "G200-3456-7890", "is_primary": true},
        {"name": "Carlos Garcia", "license_number": "G200-6543-2109", "is_primary": false}
      ]
    }
  ],
  "claims_history": []
}'::jsonb);

-- Policy 4: Active policy, 2 coverages, 2 vehicles, 2 drivers, 3 claims
INSERT INTO policy (data) VALUES ('{
  "policy_number": "POL-2024-01050",
  "status": "active",
  "effective_date": "2024-04-01",
  "expiration_date": "2025-04-01",
  "policyholder": {
    "first_name": "David",
    "last_name": "Okafor",
    "date_of_birth": "1970-02-28",
    "contact": {
      "email": "david.okafor@example.com",
      "phone": "+1-555-0511",
      "address": {
        "street": "789 Maple Drive",
        "city": "Portland",
        "state": "OR",
        "zip": "97201"
      }
    }
  },
  "coverages": [
    {"type": "liability", "limit": 750000, "deductible": 1500, "premium": 1400.00},
    {"type": "uninsured_motorist", "limit": 100000, "deductible": 0, "premium": 350.00}
  ],
  "vehicles": [
    {
      "vin": "1FTFW1ET5DFC10312",
      "year": 2020,
      "make": "Ford",
      "model": "F-150",
      "drivers": [
        {"name": "David Okafor", "license_number": "O100-7890-1234", "is_primary": true}
      ]
    },
    {
      "vin": "2HGFC2F59MH522145",
      "year": 2021,
      "make": "Honda",
      "model": "Accord",
      "drivers": [
        {"name": "Sarah Okafor", "license_number": "O100-4321-8765", "is_primary": true}
      ]
    }
  ],
  "claims_history": [
    {"claim_id": "CLM-004", "date": "2024-05-10", "amount": 8500.00, "status": "closed", "description": "Parking lot fender bender"},
    {"claim_id": "CLM-005", "date": "2024-08-22", "amount": 1500.00, "status": "closed", "description": "Hail damage"},
    {"claim_id": "CLM-006", "date": "2024-11-03", "amount": 22000.00, "status": "open", "description": "T-bone collision at red light"}
  ]
}'::jsonb);
