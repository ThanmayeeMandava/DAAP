db.createUser(
        {
            user: "<your personal username>",
            pwd: "<your personal password>",
            roles: [
                {
                    role: "readWrite",
                    db: "dap"
                }
            ]
        }
);