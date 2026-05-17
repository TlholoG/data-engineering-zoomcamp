"""
76)[Verified]
Select the appropriate attack and remediation from each drop-down list to label the corresponding attack with its remediation.

Attack Description: An attacker sends multiple SYN packets from multiple sources.
Target: Web server      
Attack Identified: *Botnet, RAT, Logic Bomb, Backdoor, Virus, Spyware, Worm, Adware, Randsomware, Keylogger, Phishing.   
Best Preventative or Remediation Action:
   *Enable DDOS protection,
    Patch vulnerable systems,
    Disable vulnerable services,
    Change the default system password
    Update the cyptographic algorythims
    Change the default application password
    Implement 2FA using push notification
    Conduct a code review
    Implement application fuzzing
    Implement a host-based IPS
    Disable remote access services

Attack Description: The attack establishes a connection. Which allows remote commands to be executed.
Target: User      
Attack Identified: Botnet, *RAT, Logic Bomb, Backdoor, Virus, Spyware, Worm, Adware, Randsomware, Keylogger, *Phishing.   
Best Preventative or Remediation Action:
    Enable DDOS protection,
    Patch vulnerable systems,
    Disable vulnerable services,
    Change the default system password
    Update the cyptographic algorythims
    Change the default application password
    Implement 2FA using push notification
    Conduct a code review
    Implement application fuzzing
   *Implement a host-based IPS
    Disable remote access services

Attack Description: The attack is self propagating and compromises a SQL database using well-known credentials as it moves through the network .
Target: Database server      
Attack Identified: Botnet, RAT, Logic Bomb, Backdoor, Virus, Spyware, *Worm, Adware, Randsomware, Keylogger, Phishing.   
Best Preventative or Remediation Action:
    Enable DDOS protection,
   *Patch vulnerable systems,
    Disable vulnerable services,
    Change the default system password
    Update the cyptographic algorythims
   *Change the default application password
    Implement 2FA using push notification
    Conduct a code review
    Implement application fuzzing
    Implement a host-based IPS
    Disable remote access services

Attack Description: The attacker uses hardware to remotely monitor a user's input activity to harvest credentials.
Target: Executive      
Attack Identified: Botnet, RAT, Logic Bomb, Backdoor, Virus, Spyware, Worm, Adware, Randsomware, *Keylogger, Phishing.   
Best Preventative or Remediation Action:
    Enable DDOS protection,
    Patch vulnerable systems,
    Disable vulnerable services,
    Change the default system password
    Update the cyptographic algorythims
    Change the default application password
   *Implement 2FA using push notification
    Conduct a code review
    Implement application fuzzing
    Implement a host-based IPS
    Disable remote access services

Attack Description: The attacker embeds hidden acces in an internally developed application that bypasses account login.
Target: Application      
Attack Identified: Botnet, RAT, Logic Bomb, *Backdoor, Virus, Spyware, Worm, Adware, Randsomware, Keylogger, Phishing.   
Best Preventative or Remediation Action:
    Enable DDOS protection,
    Patch vulnerable systems,
    Disable vulnerable services,
    Change the default system password
    Update the cyptographic algorythims
    Change the default application password
    Implement 2FA using push notification
   *Conduct a code review
    Implement application fuzzing
    Implement a host-based IPS
    Disable remote access services

77) [Verified] You are a security administrator investigating a potential infection on a network.
Review all logs to determine which host originated the infection and then identify if each remaining host is clean or infected.

__ 192.168.10.22 __
4/17/2019  14:30  Info   Scheduled scan initiated
4/17/2019  14:31  Info   Checking for update
4/17/2019  14:32  Info   No update available
4/17/2019  14:33  Info   Checking for definition update
4/17/2019  14:34  Info   No definition update available
4/17/2019  14:35  Info   Scan type = full
4/17/2019  14:36  Info   Scan start
4/17/2019  14:37  Info   Scanning system files
4/17/2019  14:38  Info   Scanning temporary files
4/17/2019  14:39  Info   Scanning services
4/17/2019  14:40  Info   Scanning boot sector
4/17/2019  14:41  Info   Scan complete
4/17/2019  14:42  Info   Files removed: 0
4/17/2019  14:43  Info   Files quarantined: 0
4/17/2019  14:44  Info   Boot sector: clean
4/17/2019  14:45  Info   Next Scheduled scan: 4/18/2019 14:30
4/18/2019  2:31   Warn   Scheduled scan disabled be process scvh0st.exe
4/18/2019  2:32   Warn   Scheduled update disabled by process scvh0st.exe

__ 192.168.10.37 __
4/17/2019  14:30  Info   Scheduled scan initiated
4/17/2019  14:31  Info   Checking for update
4/17/2019  14:32  Info   No update available
4/17/2019  14:33  Info   Checking for definition update
4/17/2019  14:34  Info   No definition update available
4/17/2019  14:35  Info   Scan type = full
4/17/2019  14:36  Info   Scan start
4/17/2019  14:37  Info   Scanning system files
4/17/2019  14:38  Info   Scanning temporary files
4/17/2019  14:39  Info   Scanning services
4/17/2019  14:40  Info   Scanning boot sector
4/17/2019  14:41  Info   Scan complete
4/17/2019  14:42  Info   Files removed: 0
4/17/2019  14:43  Info   Files quarantined: 0
4/17/2019  14:44  Info   Boot sector: clean
4/17/2019  14:45  Info   Next Scheduled scan: 4/18/2019 14:30
4/18/2019  14:30  Info   Scheduled scan initiated
4/18/2019  14:31  Info   Checking for update
4/18/2019  14:32  Info   update available
4/18/2019  14:33  Info   Checking for definition update
4/18/2019  14:34  Info   Update available v10.2.3.4440
4/18/2019  14:33  Info	 Downloading update
4/18/2019  14:35  Info   Definition update complete
4/18/2019  14:35  Info   Scan type = full
4/18/2019  14:36  Info	 Scan start
4/18/2019  14:37  Info   Scanning system files
4/18/2019  14:37  Warn 	 File found svch0st.exe match definition v10.2.3.4440
4/18/2019  14:37  Warn 	 File quarantined svch0st.exe
4/18/2019  14:38  Info   Scanning temporary files
4/18/2019  14:39  Info   Scanning services
4/18/2019  14:40  Info   Scanning boot sector
4/18/2019  14:41  Info   Scan complete
4/18/2019  14:42  Info   Files removed: 0
4/18/2019  14:43  Info   Files quarantined: 1
4/18/2019  14:44  Info   Boot sector: clean
4/18/2019  14:45  Info   Next Scheduled scan: 4/19/2019 14:30

__ 192.168.10.41 __
4/17/2019  14:30  Info   Scheduled scan initiated
4/17/2019  14:31  Info   Checking for update
4/17/2019  14:32  Info   No update available
4/17/2019  14:33  Info   Checking for definition update
4/17/2019  14:34  Info   No definition update available
4/17/2019  14:35  Info   Scan type = full
4/17/2019  14:36  Info   Scan start
4/17/2019  14:37  Info   Scanning system files
4/17/2019  14:38  Info   Scanning temporary files
4/17/2019  14:39  Info   Scanning services
4/17/2019  14:40  Info   Scanning boot sector
4/17/2019  14:41  Info   Scan complete
4/17/2019  14:42  Info   Files removed: 0
4/17/2019  14:43  Info   Files quarantined: 0
4/17/2019  14:44  Info   Boot sector: clean
4/17/2019  14:45  Info   Next Scheduled scan: 4/18/2019 14:30
4/18/2019  14:30  Info   Scheduled scan initiated
4/18/2019  14:31  Info   Checking for update
4/18/2019  14:32  Info   No update available
4/18/2019  14:33  Info   Checking for definition update
4/18/2019  14:34  Error  Unable to reach update server
4/18/2019  14:35  Info   Scan type = full
4/18/2019  14:36  Info	 Scan start
4/18/2019  14:37  Info	 Scanning system files
4/18/2019  14:37  Warn	 File found svch0st.exe match definition v10.2.3.4440
4/18/2019  14:37  Error	 Unable to quarantined file svch0st.exe
4/18/2019  14:37  Info   Scanning temporary files
4/18/2019  14:39  Info   Scanning services
4/18/2019  14:40  Info   Scanning boot sector
4/18/2019  14:41  Info   Scan complete
4/18/2019  14:42  Info   Files removed: 0
4/18/2019  14:43  Info   Files quarantined: 0
4/18/2019  14:43  Warn   File quarantine file
4/18/2019  14:44  Info   Boot sector: clean
4/18/2019  14:45  Info   Next Scheduled scan: 4/19/2019 14:30

__ 10.10.9.12 __
4/17/2019  14:30  Info   Scheduled scan initiated
4/17/2019  14:31  Info   Checking for update
4/17/2019  14:32  Info   No update available
4/17/2019  14:33  Info   Checking for definition update
4/17/2019  14:34  Info   No definition update available
4/17/2019  14:35  Info   Scan type = full
4/17/2019  14:36  Info   Scan start
4/17/2019  14:37  Info   Scanning system files
4/17/2019  14:38  Info   Scanning temporary files
4/17/2019  14:39  Info   Scanning services
4/17/2019  14:40  Info   Scanning boot sector
4/17/2019  14:41  Info   Scan complete
4/17/2019  14:42  Info   Files removed: 0
4/17/2019  14:43  Info   Files quarantined: 0
4/17/2019  14:44  Info   Boot sector: clean
4/17/2019  14:45  Info   Next Scheduled scan: 4/18/2019 14:30
4/18/2019  14:30  Info   Scheduled scan initiated
4/18/2019  14:31  Info   Checking for update
4/18/2019  14:32  Info   update available
4/18/2019  14:33  Info   Checking for definition update
4/18/2019  14:34  Info   Update available v10.2.3.4440
4/18/2019  14:33  Info	 Downloading update
4/18/2019  14:35  Info   Definition update complete
4/18/2019  14:35  Info   Scan type = full
4/18/2019  14:36  Info	 Scan start
4/18/2019  14:37  Info   Scanning system files
4/18/2019  14:37  Warn 	 File found svch0st.exe match definition v10.2.3.4440
4/18/2019  14:37  Warn 	 File quarantined svch0st.exe
4/18/2019  14:38  Info   Scanning temporary files
4/18/2019  14:39  Info   Scanning services
4/18/2019  14:40  Info   Scanning boot sector
4/18/2019  14:41  Info   Scan complete
4/18/2019  14:42  Info   Files removed: 0
4/18/2019  14:43  Info   Files quarantined: 1
4/18/2019  14:44  Info   Boot sector: clean
4/18/2019  14:45  Info   Next Scheduled scan: 4/19/2019 14:30

__ 10.10.9.18 __
4/17/2019  14:30  Info   Scheduled scan initiated
4/17/2019  14:31  Info   Checking for update
4/17/2019  14:32  Info   No update available
4/17/2019  14:33  Info   Checking for definition update
4/17/2019  14:34  Info   No definition update available
4/17/2019  14:35  Info   Scan type = full
4/17/2019  14:36  Info   Scan start
4/17/2019  14:37  Info   Scanning system files
4/17/2019  14:38  Info   Scanning temporary files
4/17/2019  14:39  Info   Scanning services
4/17/2019  14:40  Info   Scanning boot sector
4/17/2019  14:41  Info   Scan complete
4/17/2019  14:42  Info   Files removed: 0
4/17/2019  14:43  Info   Files quarantined: 0
4/17/2019  14:44  Info   Boot sector: clean
4/17/2019  14:45  Info   Next Scheduled scan: 4/18/2019 14:30
4/18/2019  14:30  Info   Scheduled scan initiated
4/18/2019  14:31  Info   Checking for update
4/18/2019  14:32  Info   No update available
4/18/2019  14:33  Info   Checking for definition update
4/18/2019  14:34  Error  Unable to reach update server
4/18/2019  14:35  Info   Scan type = full
4/18/2019  14:36  Info	 Scan start
4/18/2019  14:37  Info	 Scanning system files
4/18/2019  14:37  Warn	 File found svch0st.exe match definition v10.2.3.4440
4/18/2019  14:37  Error	 Unable to quarantined file svch0st.exe
4/18/2019  14:38  Info   Scanning temporary files
4/18/2019  14:39  Info   Scanning services
4/18/2019  14:40  Info   Scanning boot sector
4/18/2019  14:41  Info   Scan complete
4/18/2019  14:42  Info   Files removed: 0
4/18/2019  14:43  Info   Files quarantined: 0
4/18/2019  14:43  Warn   File quarantine file
4/18/2019  14:44  Info   Boot sector: clean
4/18/2019  14:45  Info   Next Scheduled scan: 4/19/2019 14:30

__ Firewall __
Timestamp             Source         Destination    Destination Port  Application   Action  Client Bytes   Server Bytes
4/17/2019  16:01:44   10.10.9.18     57.203.54.183  443               ssl           Permit  6953           99427
4/17/2019  16:01:58   192.168.10.37  57.203.54.221  443               ssl           Permit  9301           199386
4/17/2019  16:17:06   192.168.10.22  10.10.9.12     135               rpc           Permit  175            1504
4/17/2019  16:27:36   192.168.10.41  10.10.9.12     445               smbv1         Permit  345            34757
4/17/2019  16:28:06   10.10.9.12     192.168.10.41  135               rpc           Permit  754            4771
4/17/2019  16:33:31   10.10.9.18     192.168.10.22  135               rpc           Permit  643            2355
4/17/2019  16:35:36   192.168.10.37  10.10.9.12     135               smbv2         Permit  649            5644
4/17/2019  23:58:36   10.10.9.12     192.168.10.41                    icmp          Permit  128            128
4/17/2019  23:58:43   10.10.9.12     192.168.10.22                    icmp          Permit  128            128
4/17/2019  23:58:45   10.10.9.12     192.168.10.37                    icmp          Permit  128            128
4/18/2019  2:31:36    10.10.9.18     192.168.10.41  445               smbv2         Permit  1874           23874
4/18/2019  2:31:45    192.168.10.22  57.203.55.29   8080              http          Permit  7203           75997
4/18/2019  2:31:51    10.10.9.18     57.203.56.201  443               ssl           Permit  9953           199730
4/18/2019  2:31:02    192.168.10.22  57.203.55.234  443               http          Permit  4937           84937
4/18/2019  2:39:11    192.168.10.41  57.203.53.89   8080              http          Permit  8201           133183
4/18/2019  2:39:12    10.10.9.18     57.203.55.19   8080              ssl           Permit  1284           9102854
4/18/2019  2:39:32    192.168.10.37  57.203.56.113  443               ssl           Permit  9341           9938
4/18/2019  13:37:36   192.168.10.22  10.10.9.18     445               smbv3         Permit  1874           23874
4/18/2019  13:39:43   192.168.10.22  10.10.9.18     135               rpc           Permit  673            41358
4/18/2019  13:45:04   10.10.9.18     192.168.10.37  135               rpc           Permit  693            1952
4/18/2019  13:47:44   10.10.9.12     192.168.10.41  445               smbv3         Permit  482            3505
4/18/2019  13:52:57   10.10.9.12     192.168.10.22  135               rpc           Permit  545            90063
4/18/2019  13:53:01   192.168.10.37  10.10.9.12     335               smbv3         Permit  876            8068
4/18/2019  14:30:04   10.10.9.12     57.203.56.231  443               ssl           Permit  9901           199730
4/18/2019  14:30:04   192.168.10.37  57.203.56.143  443               ssl           Permit  10092          209938


192.168.10.22; *Origin, *Infected, Clean
192.168.10.37; Origin, Infected, *Clean
192.168.10.41; Origin, *Infected, Clean

10.10.9.12; Origin, Infected, *Clean
10.10.9.18; Origin, *Infected, Clean

78) 
Which of the following is the phase in the incident response process when a security analyst reviews roles and responsibilities?
C. Lessons learned

79)
After a recent vulnerability scan, a security engineer needs to harden the routers within the corporate network. 
Which of the following is the most appropriate to disable?
D. Web-based administration

80)
A security administrator needs a method to secure data in an environment that includes some form of checks so track any changes. 
Which of the following should the administrator set up to achieve this goal?
D. FIM (File Integrity Monitoring)

81)

An administrator is reviewing a single server's security logs and discovers the following:
Keywords        Date and Time            Source                      Event ID   Task Category
Audit Failure   09/16/2022 11:13:05 AM   Microsoft Windows Security  4625       Logon
Audit Failure   09/16/2022 11:13:07 AM   Microsoft Windows Security  4625       Logon
Audit Failure   09/16/2022 11:13:09 AM   Microsoft Windows Security  4625       Logon
Audit Failure   09/16/2022 11:13:11 AM   Microsoft Windows Security  4625       Logon
Audit Failure   09/16/2022 11:13:13 AM   Microsoft Windows Security  4625       Logon
Audit Failure   09/16/2022 11:13:15 AM   Microsoft Windows Security  4625       Logon
Audit Failure   09/16/2022 11:13:17 AM   Microsoft Windows Security  4625       Logon
Audit Failure   09/16/2022 11:13:19 AM   Microsoft Windows Security  4625       Logon
Audit Failure   09/16/2022 11:13:21 AM   Microsoft Windows Security  4625       Logon
Audit Failure   09/16/2022 11:13:23 AM   Microsoft Windows Security  4625       Logon
Audit Failure   09/16/2022 11:13:25 AM   Microsoft Windows Security  4625       Logon
Audit Failure   09/16/2022 11:13:27 AM   Microsoft Windows Security  4625       Logon

Which of the following best describes the action captured in this log file?

82)
A security engineer is implementing FDE for all laptops in an organization. 
Which of the following are the most important for the engineer to consider as part of the planning process? (Choose two.)

A. Key escrow
B. TPM presence

83)
A security analyst scans a company's public network and discovers a host is running a remote desktop that can be used to access the production network. 
Which of the following changes should the security analyst recommend?

B. Setting up a VPN and placing the jump server inside the firewall

84)
An enterprise has been experiencing attacks focused on exploiting vulnerabilities in older browser versions with well-known exploits. 
Which of the following security solutions should be configured to best provide the ability to monitor and block these known signature-based attacks?

D. IPS (Intrusion Prevention System) → Detects and actively blocks attacks using signatures.










"""