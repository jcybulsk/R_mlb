# Conduct a query over multiple seaasons to compile stats 
# on hitters and perform a clustering analysis on them

library(ape)
library(ggplot2)
library(MASS)
library(sm)

# Connect to the SQL server hosting the data
library(RMySQL)
con <- dbConnect(RMySQL::MySQL(), dbname = "stats", 
                 host = "localhost", 
                 username = "root", 
                 password = "root", 
                 unix.socket = "/Applications/MAMP/tmp/mysql/mysql.sock", 
                 port = 3306)

# Set a vector of years to loop over, and perform the query for each year.
# Within each year, also remove duplicate rows and merge player stats if 
# they changed teams. Then merge the data for a single player in consecutive
# years.

yrs <- c(1965:2015)
for(j in 1:length(yrs)){
    # Here's my SQL query for each year.
    str1 <- "SELECT bat.playerID, mas.nameFirst, mas.nameLast, bat.yearID, 
    bat.teamID, bat.G, bat.AB, bat.R, bat.H, bat.2B, bat.3B, bat.HR, bat.RBI, 
    bat.SB, bat.CS, bat.BB, bat.SO, bat.IBB, bat.HBP, bat.SH, bat.SF, bat.GIDP, 
    app.G_all, app.G_p
    FROM batting as bat, master as mas, appearances as app
    WHERE mas.playerID=bat.playerID AND app.playerID=mas.playerID AND 
    bat.yearID="
    str2 <- as.character(yrs[j])
    str3 <- ";"
    all.query <- dbGetQuery(con, paste(c(str1, str2, str3), collapse = ''))
    
    # First! Before going forward, change the 2B, 3B labels to "doubles" and "triples"
    colnames(all.query)[10] <- "doubles"
    colnames(all.query)[11] <- "triples"
    
    ### Some of these (IBB, HBP, SH, SF, GIDP, etc.) all are stored as 
    ### characters, and must be converted into floats/doubles. 
    all.query$AB <- as.double(all.query$AB)
    all.query$IBB <- as.double(all.query$IBB)
    all.query$HBP <- as.double(all.query$HBP)
    all.query$SH <- as.double(all.query$SH)
    all.query$SF <- as.double(all.query$SF)
    all.query$GIDP <- as.double(all.query$GIDP)
    
    # Something that needs to be dealt with. I have at least a few cases where a row is 
    # repeated, i.e., where we have the same player and team with their stats repeated.
    # Note that this is separate from the obvious cases where a player was traded, and so 
    # has their season stats split amongst multiple teams. I should really deal with both 
    # cases right here, though. 
    # Step 1: if there are duplicates of a playerID AND team, erase the duplicate row(s)
    # Step 2: if a player was on two teams, add their relevant stats together and make their 
    #         teamID a new one: TEAMID1/TEAMID2
    
    play.id.array <- all.query$playerID
    team.id.array <- all.query$teamID
    play.team.merge <- paste(play.id.array, team.id.array)
    all.query <- all.query[!duplicated(play.team.merge),]
    
    # Now, the remaining cases of duplicated playerIDs must be cases of a trade mid-season
    # or a player being released and picked up by another team
    the.dupes <- all.query[duplicated(all.query$playerID), ]
    for(i in 1:nrow(the.dupes)){
        # merge the stats with i index and the previous index, and update the teamID
        all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$teamID <- 
            paste(c(all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$teamID, 
                    "/", all.query[all.query$playerID == the.dupes$playerID[i], ][2,]$teamID), 
                  collapse = '')
        
        all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$G <- 
            all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$G + 
            all.query[all.query$playerID == the.dupes$playerID[i], ][2,]$G
        
        all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$AB <- 
            all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$AB + 
            all.query[all.query$playerID == the.dupes$playerID[i], ][2,]$AB
        
        all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$R <- 
            all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$R + 
            all.query[all.query$playerID == the.dupes$playerID[i], ][2,]$R
        
        all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$H <- 
            all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$H + 
            all.query[all.query$playerID == the.dupes$playerID[i], ][2,]$H
        
        all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$doubles <- 
            all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$doubles + 
            all.query[all.query$playerID == the.dupes$playerID[i], ][2,]$doubles
        
        all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$triples <- 
            all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$triples + 
            all.query[all.query$playerID == the.dupes$playerID[i], ][2,]$triples    
        
        all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$HR <- 
            all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$HR + 
            all.query[all.query$playerID == the.dupes$playerID[i], ][2,]$HR    
        
        all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$RBI <- 
            all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$RBI + 
            all.query[all.query$playerID == the.dupes$playerID[i], ][2,]$RBI
        
        all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$SB <- 
            all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$SB + 
            all.query[all.query$playerID == the.dupes$playerID[i], ][2,]$SB
        
        all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$CS <- 
            all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$CS + 
            all.query[all.query$playerID == the.dupes$playerID[i], ][2,]$CS    
        
        all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$BB <- 
            all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$BB + 
            all.query[all.query$playerID == the.dupes$playerID[i], ][2,]$BB    
        
        all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$SO <- 
            all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$SO + 
            all.query[all.query$playerID == the.dupes$playerID[i], ][2,]$SO    
        
        all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$IBB <- 
            all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$IBB + 
            all.query[all.query$playerID == the.dupes$playerID[i], ][2,]$IBB    
        
        all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$HBP <- 
            all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$HBP + 
            all.query[all.query$playerID == the.dupes$playerID[i], ][2,]$HBP    
        
        all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$SH <- 
            all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$SH + 
            all.query[all.query$playerID == the.dupes$playerID[i], ][2,]$SH
        
        all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$SF <- 
            all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$SF + 
            all.query[all.query$playerID == the.dupes$playerID[i], ][2,]$SF    
        
        all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$GIDP <- 
            all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$GIDP + 
            all.query[all.query$playerID == the.dupes$playerID[i], ][2,]$GIDP    

        all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$G_all <- 
            all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$G_all + 
            all.query[all.query$playerID == the.dupes$playerID[i], ][2,]$G_all

        all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$G_p <- 
            all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$G_p + 
            all.query[all.query$playerID == the.dupes$playerID[i], ][2,]$G_p
    }
    
    ### Finally, remove all duplicates now that the stats have been merged!
    all.query <- all.query[!duplicated(all.query$playerID),]

    # If this is the first time, just store the overall.query as the current 
    # query results. If not, then we've gotta do some merging of the tables
    if(j == 1){
        overall.query <- all.query
    }
    if(j > 1){
        overall.new <- rbind(overall.query, all.query)
        the.dupes <- overall.new[duplicated(overall.new$playerID), ]
        for(i in 1:nrow(the.dupes)){
            # merge the stats with i index and the previous index, and update the teamID
            overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$G <- 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$G + 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][2,]$G
            
            overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$AB <- 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$AB + 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][2,]$AB
            
            overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$R <- 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$R + 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][2,]$R
            
            overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$H <- 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$H + 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][2,]$H
            
            overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$doubles <- 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$doubles + 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][2,]$doubles
            
            overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$triples <- 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$triples + 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][2,]$triples    
            
            overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$HR <- 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$HR + 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][2,]$HR    
            
            overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$RBI <- 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$RBI + 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][2,]$RBI
            
            overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$SB <- 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$SB + 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][2,]$SB
            
            overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$CS <- 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$CS + 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][2,]$CS    
            
            overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$BB <- 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$BB + 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][2,]$BB    
            
            overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$SO <- 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$SO + 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][2,]$SO    
            
            overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$IBB <- 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$IBB + 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][2,]$IBB    
            
            overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$HBP <- 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$HBP + 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][2,]$HBP    
            
            overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$SH <- 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$SH + 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][2,]$SH
            
            overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$SF <- 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$SF + 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][2,]$SF    
            
            overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$GIDP <- 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$GIDP + 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][2,]$GIDP

            overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$G_all <- 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$G_all + 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][2,]$G_all
            
            overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$G_p <- 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$G_p + 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][2,]$G_p
        }
        
        # Now take out the duplicates, and save it as overall.query and move on
        overall.query <- overall.new[!duplicated(overall.new$playerID),]
    }
}

# Okay, one little thing here that's kind of annoying, there are two players named 
# "Alex Gonzalez", having playerIDs gonzaal01 and gonzaal02. The player with ID 
# gonzaal01 is Alex S. Gonzalez, so I would just add a " S." to the first name string.
overall.query$nameFirst[overall.query$playerID == "gonzaal01"] <- paste("Alex", " S.")

# And after running this over the whole span of 1965-2015, I find four more names 
# are duplicated: Sandy Alomar, Ken Griffey, Jose Cruz, and Gary Matthews. All are 
# juniors, as it happens.
overall.query$nameLast[overall.query$playerID == "alomasa02"] <- "Alomar Jr."
overall.query$nameLast[overall.query$playerID == "griffke02"] <- "Griffey Jr."
overall.query$nameLast[overall.query$playerID == "cruzjo02"] <- "Cruz Jr."
overall.query$nameLast[overall.query$playerID == "matthga02"] <- "Matthews Jr."

# Select just those hitters who have more than 2500 AB over this time period
hitters <- overall.query[overall.query$AB > 2500,]

nseasons <- hitters$G/162.0
sb_per_season <- (hitters$SB/nseasons)/max(hitters$SB/nseasons)
sing <- hitters$H - hitters$doubles - hitters$triples - hitters$HR
obp <- (hitters$H + hitters$BB + hitters$HBP)/(hitters$AB + hitters$BB + hitters$HBP + hitters$SF)
tb <- sing + (2.0*hitters$doubles) + (3.0*hitters$triples) + (4.0*hitters$HR)
ba <- hitters$H/hitters$AB
slg <- tb/hitters$AB
ops <- obp+slg
iso <- slg-ba
so_per_ab <- (hitters$SO/hitters$AB)/max(hitters$SO/hitters$AB)
hr_per_ab <- (hitters$HR/hitters$AB)/max(hitters$HR/hitters$AB)
bb_per_ab <- (hitters$BB/hitters$AB)/max(hitters$BB/hitters$AB)
doubles_per_ab <- (hitters$doubles/hitters$AB)/max(hitters$doubles/hitters$AB)

# Now append all this stuff I just calculated to the data frame of hitters:
hitters <- cbind(hitters, sing, obp, 
                 tb, ba, slg, ops, 
                 iso, so_per_ab, hr_per_ab, 
                 doubles_per_ab,  
                 bb_per_ab, sb_per_season)
colnames(hitters)

# Before making plots, make a nice set of player names for easy display in the 
# upcoming figures
hitter.names <- rep("", times = nrow(hitters))

# Set the names equal to a concatenation of firstname and lastname
for(i in 1:nrow(hitters)){
    hitter.names[i] <- paste(c(hitters$nameFirst[i], " ", 
                               hitters$nameLast[i]), collapse='')
}

# Now onto the calculations. The stats I'll use come from 
# c(30, 36, 32, 35, 34, 33)
hitter.data <- hitters[, c(30, 36, 32, 35, 34, 33)]

# Get the distance matrix between all stats for all players
d <- dist(hitter.data)

# Compute the hierarchical clustering for those distances
c <- hclust(d)

# Now select seven groups
g7 <- cutree(c, k = 7)

# Convert to a phylogenetic tree and assign labels as hitter names
phylo.tree <- as.phylo(c)

phylo.tree$tip.label <- hitter.names

require("RColorBrewer")
display.brewer.pal(7, "Set1")

the.colors <- brewer.pal(7, "Set1")[unclass(g7)]

# One thing I'd like to do. I think the yellow color is too bright, so 
# I want to replace every instance of #FFFF33 with #C6C629
the.colors[the.colors == "#FFFF33"] <- "#C6C629"

# Make the fan plot, with colors corresponding to each group
plot(phylo.tree, 
     type = "fan", 
     cex=0.5,  
     tip.color = the.colors)

# Now make a scatterplot matrix that displays all the data points for the 
# parameters I'm tracking in my clustering analysis
pairs(hitters[c(30, 36, 32, 35, 34, 33)], 
    main = "Scatterplot Matrix for Hitting Stats", 
    col = the.colors, 
    pch = 16, 
    cex = 1.0, 
    cex.labels = 2.5, 
    labels = c("OPS", "SB/season", 
               "SO/AB", "BB/AB", "2B/AB", 
               "HR/AB"))


# I should add "group" onto the dataframe hitters, which will make 
# the labels of a set of boxplots separated by group, automatically 
# show the right names.
groups <- g7
groups[groups == "1"] <- "Group 1"
groups[groups == "2"] <- "Group 2"
groups[groups == "3"] <- "Group 3"
groups[groups == "4"] <- "Group 4"
groups[groups == "5"] <- "Group 5"
groups[groups == "6"] <- "Group 6"
groups[groups == "7"] <- "Group 7"
hittersplot <- cbind(hitters, groups)
colnames(hittersplot)

color.example <- brewer.pal(7, "Set1")
color.example[color.example == "#FFFF33"] <- "#C6C629"

# Make a grid of figures. Three columns and two rows, with 
# each showing boxplots of the distributions of each stat
# separated by group.
par(mfrow=c(2,3), oma=c(0,0,0,0), mar=c(6,6,6,6))

# The figure below looks really funky in the display panel for 
# RStudio, but when saved as a figure with width =1200 pix
# it looks pretty okay.
boxplot(ops~groups, data=hittersplot, 
        col=color.example, main="OPS", 
        boxwex=0.5, pch=16, outcol=color.example, 
        whisklty=1, cex.lab=1.3, cex.axis=1.8, cex.main=2.5)

# I considered adding a horizontal line to each of these panels 
# to show where the median stat value is for the overall hitter 
# sample, but ultimately it's too distracting.
#abline(h=median(hittersplot$ops), lty=2, lwd=2)

boxplot(sb_per_season~groups, data=hittersplot, 
        col=color.example, main="Stolen Bases Per Season\n (Normalized)", 
        boxwex=0.5, pch=16, outcol=color.example, 
        whisklty=1, cex.lab=1.3, cex.axis=1.8, cex.main=2.5)

#abline(h=median(hittersplot$sb_per_season), lty=2, lwd=2)

boxplot(so_per_ab~groups, data=hittersplot, 
        col=color.example, main="Strikeouts Per At-Bat\n (Normalized)", 
        boxwex=0.5, pch=16, outcol=color.example, 
        whisklty=1, cex.lab=1.3, cex.axis=1.8, cex.main=2.5)

#abline(h=median(hittersplot$so_per_ab), lty=2, lwd=2)

boxplot(bb_per_ab~groups, data=hittersplot, 
        col=color.example, main="Walks Per At-Bat\n (Normalized)", 
        boxwex=0.5, pch=16, outcol=color.example, 
        whisklty=1, cex.lab=1.3, cex.axis=1.8, cex.main=2.5)

#abline(h=median(hittersplot$bb_per_ab), lty=2, lwd=2)

boxplot(doubles_per_ab~groups, data=hittersplot, 
        col=color.example, main="Doubles Per At-Bat\n (Normalized)", 
        boxwex=0.5, pch=16, outcol=color.example, 
        whisklty=1, cex.lab=1.3, cex.axis=1.8, cex.main=2.5)

#abline(h=median(hittersplot$doubles_per_ab), lty=2, lwd=2)

boxplot(hr_per_ab~groups, data=hittersplot, 
        col=color.example, main="Homeruns Per At-Bat\n (Normalized)", 
        boxwex=0.5, pch=16, outcol=color.example, 
        whisklty=1, cex.lab=1.3, cex.axis=1.8, cex.main=2.5)

#abline(h=median(hittersplot$hr_per_ab), lty=2, lwd=2)

boxplot(HR/doubles~groups, data=hittersplot, 
        col=color.example, main="HR/2B Ratio", 
        boxwex=0.5, pch=16, outcol=color.example, 
        whisklty=1, cex.lab=1.2, cex.axis=2.0, cex.main=2.5)

# Restore defaults (one plot at a time)!
par(mfrow=c(1,1))

# We can many any other boxplot figures we want now, separated by group. For 
# example, the ratio of times a hitter has grounded into a double play per 
# at-bat?
boxplot(GIDP/AB~groups, data=hittersplot, 
        col=color.example, main="GIDP per AB", 
        boxwex=0.5, pch=16, outcol=color.example, 
        whisklty=1, cex.lab=1.2, cex.axis=2.0, cex.main=2.5)


hitters1 <- hitters[g7 == 1,]
hitters2 <- hitters[g7 == 2,]
hitters3 <- hitters[g7 == 3,]
hitters4 <- hitters[g7 == 4,]
hitters5 <- hitters[g7 == 5,]
hitters6 <- hitters[g7 == 6,]
hitters7 <- hitters[g7 == 7,]

# One last little thing. I'm referencing these groups on a post on my website, and 
# so to re-create the group colors as precisely as possible I should get the HEX 
# codes for them
col1 <- GetColorHexAndDecimal(color.example[1])
col2 <- GetColorHexAndDecimal(color.example[2])
col3 <- GetColorHexAndDecimal(color.example[3])
col4 <- GetColorHexAndDecimal(color.example[4])
col5 <- GetColorHexAndDecimal(color.example[5])
col6 <- GetColorHexAndDecimal(color.example[6])
col7 <- GetColorHexAndDecimal(color.example[7])
