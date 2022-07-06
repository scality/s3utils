const getRepairStrategy = require('../../../../CompareRaftMembers/RepairObjects/getRepairStrategy');

describe('RepairObjects.getRepairStrategy()', () => {
    [
        {
            desc: 'missing on follower',
            followerState: {
                diffMd: null,
                isReadable: false,
            },
            leaderState: {
                diffMd: '{"foo":"bar"}',
                isReadable: true,
                refreshedMd: '{"foo":"bar"}',
            },
            expectedRepairStatus: 'AutoRepair',
            expectedRepairSource: 'Leader',
            expectedRepairMaster: false,
        },
        {
            desc: 'missing on follower as latest version',
            followerState: {
                diffMd: null,
                isReadable: false,
            },
            leaderState: {
                diffMd: '{"foo":"bar","versionId":"1234"}',
                isReadable: true,
                refreshedMd: '{"foo":"bar","versionId":"1234"}',
                refreshedMasterMd: '{"foo":"bar","versionId":"1234"}',
            },
            expectedRepairStatus: 'AutoRepair',
            expectedRepairSource: 'Leader',
            expectedRepairMaster: true,
        },
        {
            desc: 'missing on follower as non-latest version',
            followerState: {
                diffMd: null,
                isReadable: false,
            },
            leaderState: {
                diffMd: '{"foo":"bar","versionId":"1234"}',
                isReadable: true,
                refreshedMd: '{"foo":"bar","versionId":"1234"}',
                refreshedMasterMd: '{"foo":"bar","versionId":"1111"}',
            },
            expectedRepairStatus: 'AutoRepair',
            expectedRepairSource: 'Leader',
            expectedRepairMaster: false,
        },
        {
            desc: 'unreadable on follower',
            followerState: {
                diffMd: '{"foo":"bar"}',
                isReadable: false,
            },
            leaderState: {
                diffMd: '{"foo":"bar"}',
                isReadable: true,
                refreshedMd: '{"foo":"bar"}',
            },
            expectedRepairStatus: 'AutoRepair',
            expectedRepairSource: 'Leader',
            expectedRepairMaster: false,
        },
        {
            desc: 'missing on follower and changed on leader',
            followerState: {
                diffMd: null,
                isReadable: false,
            },
            leaderState: {
                diffMd: '{"foo":"bar"}',
                isReadable: true,
                refreshedMd: '{"newfoo":"newbar"}',
            },
            expectedRepairStatus: 'UpdatedByClient',
        },
        {
            desc: 'unreadable on follower and changed on leader',
            followerState: {
                diffMd: '{"foo":"bar"}',
                isReadable: false,
            },
            leaderState: {
                diffMd: '{"foo":"bar"}',
                isReadable: true,
                refreshedMd: '{"newfoo":"newbar"}',
            },
            expectedRepairStatus: 'UpdatedByClient',
        },
        {
            desc: 'unreadable on follower and reappearing on leader',
            followerState: {
                diffMd: '{"foo":"bar"}',
                isReadable: false,
            },
            leaderState: {
                diffMd: null,
                isReadable: false,
                refreshedMd: '{"newfoo":"newbar"}',
            },
            expectedRepairStatus: 'UpdatedByClient',
        },
        {
            desc: 'missing on leader',
            followerState: {
                diffMd: '{"foo":"bar"}',
                isReadable: true,
            },
            leaderState: {
                diffMd: null,
                isReadable: false,
                refreshedMd: null,
            },
            expectedRepairStatus: 'AutoRepair',
            expectedRepairSource: 'Follower',
            expectedRepairMaster: false,
        },
        {
            desc: 'missing on leader with existing and same-version master',
            followerState: {
                diffMd: '{"foo":"bar","versionId":"1234"}',
                isReadable: true,
            },
            leaderState: {
                diffMd: null,
                isReadable: false,
                refreshedMd: null,
                refreshedMasterMd: '{"foo":"bar","versionId":"1234"}',
            },
            expectedRepairStatus: 'AutoRepair',
            expectedRepairSource: 'Follower',
            expectedRepairMaster: true,
        },
        {
            desc: 'missing on leader with existing but different-version master',
            followerState: {
                diffMd: '{"foo":"bar","versionId":"1234"}',
                isReadable: true,
            },
            leaderState: {
                diffMd: null,
                isReadable: false,
                refreshedMd: null,
                refreshedMasterMd: '{"otherfoo":"otherbar","versionId":"1111"}',
            },
            expectedRepairStatus: 'AutoRepair',
            expectedRepairSource: 'Follower',
            expectedRepairMaster: false,
        },
        {
            desc: 'unreadable on leader',
            followerState: {
                diffMd: '{"foo":"bar"}',
                isReadable: true,
            },
            leaderState: {
                diffMd: '{"foo":"bar"}',
                isReadable: false,
                refreshedMd: '{"foo":"bar"}',
            },
            expectedRepairStatus: 'AutoRepair',
            expectedRepairSource: 'Follower',
            expectedRepairMaster: false,
        },
        {
            desc: 'unreadable on leader as latest version',
            followerState: {
                diffMd: '{"foo":"bar","versionId":"1234"}',
                isReadable: true,
            },
            leaderState: {
                diffMd: '{"foo":"bar","versionId":"1234"}',
                isReadable: false,
                refreshedMd: '{"foo":"bar","versionId":"1234"}',
                refreshedMasterMd: '{"foo":"bar","versionId":"1234"}',
            },
            expectedRepairStatus: 'AutoRepair',
            expectedRepairSource: 'Follower',
            expectedRepairMaster: true,
        },
        {
            desc: 'unreadable on leader as non-latest version',
            followerState: {
                diffMd: '{"foo":"bar","versionId":"1234"}',
                isReadable: true,
            },
            leaderState: {
                diffMd: '{"foo":"bar","versionId":"1234"}',
                isReadable: false,
                refreshedMd: '{"foo":"bar","versionId":"1234"}',
                refreshedMasterMd: '{"foo":"bar","versionId":"1111"}',
            },
            expectedRepairStatus: 'AutoRepair',
            expectedRepairSource: 'Follower',
            expectedRepairMaster: false,
        },
        {
            desc: 'missing on leader but reappearing on leader',
            followerState: {
                diffMd: '{"foo":"bar"}',
                isReadable: true,
            },
            leaderState: {
                diffMd: null,
                isReadable: false,
                refreshedMd: '{"newfoo":"newbar"}',
            },
            expectedRepairStatus: 'UpdatedByClient',
        },
        {
            desc: 'unreadable on leader but changed on leader',
            followerState: {
                diffMd: '{"foo":"bar"}',
                isReadable: true,
            },
            leaderState: {
                diffMd: '{"foo":"bar"}',
                isReadable: false,
                refreshedMd: '{"newfoo":"newbar"}',
            },
            expectedRepairStatus: 'UpdatedByClient',
        },
        {
            desc: 'unreadable on leader but disappearing on leader',
            followerState: {
                diffMd: '{"foo":"bar"}',
                isReadable: true,
            },
            leaderState: {
                diffMd: '{"foo":"bar"}',
                isReadable: false,
                refreshedMd: null,
            },
            expectedRepairStatus: 'UpdatedByClient',
        },
        {
            desc: 'readable on leader and follower',
            followerState: {
                diffMd: '{"foo":"bar"}',
                isReadable: true,
            },
            leaderState: {
                diffMd: '{"foo":"bar"}',
                isReadable: true,
                refreshedMd: '{"foo":"bar"}',
            },
            expectedRepairStatus: 'ManualRepair',
        },
        {
            desc: 'readable on leader and follower but changed on leader',
            followerState: {
                diffMd: '{"foo":"bar"}',
                isReadable: true,
            },
            leaderState: {
                diffMd: '{"foo":"bar"}',
                isReadable: true,
                refreshedMd: '{"newfoo":"newbar"}',
            },
            expectedRepairStatus: 'UpdatedByClient',
        },
        {
            desc: 'readable on leader and follower but disappearing on leader',
            followerState: {
                diffMd: '{"foo":"bar"}',
                isReadable: true,
            },
            leaderState: {
                diffMd: '{"foo":"bar"}',
                isReadable: true,
                refreshedMd: null,
            },
            expectedRepairStatus: 'UpdatedByClient',
        },
        {
            desc: 'missing on follower and unreadable on leader',
            followerState: {
                diffMd: null,
                isReadable: false,
            },
            leaderState: {
                diffMd: '{"foo":"bar"}',
                isReadable: false,
                refreshedMd: '{"foo":"bar"}',
            },
            expectedRepairStatus: 'NotRepairable',
        },
        {
            desc: 'missing on leader and unreadable on follower',
            followerState: {
                diffMd: '{"foo":"bar"}',
                isReadable: false,
            },
            leaderState: {
                diffMd: null,
                isReadable: false,
                refreshedMd: null,
            },
            expectedRepairStatus: 'NotRepairable',
        },
        {
            desc: 'unreadable on both leader and follower',
            followerState: {
                diffMd: '{"foo":"bar"}',
                isReadable: false,
            },
            leaderState: {
                diffMd: '{"foo":"bar"}',
                isReadable: false,
                refreshedMd: '{"foo":"bar"}',
            },
            expectedRepairStatus: 'NotRepairable',
        },
        {
            desc: 'unreadable on both leader and follower but changed on leader',
            followerState: {
                diffMd: '{"foo":"bar"}',
                isReadable: false,
            },
            leaderState: {
                diffMd: '{"foo":"bar"}',
                isReadable: false,
                refreshedMd: '{"newfoo":"newbar"}',
            },
            expectedRepairStatus: 'UpdatedByClient',
        },
        {
            desc: 'unreadable on both leader and follower but disappearing on leader',
            followerState: {
                diffMd: '{"foo":"bar"}',
                isReadable: false,
            },
            leaderState: {
                diffMd: '{"foo":"bar"}',
                isReadable: false,
                refreshedMd: null,
            },
            expectedRepairStatus: 'UpdatedByClient',
        },
    ].forEach(testCase => {
        const keyDesc = testCase.leaderState.refreshedMasterMd ? 'versioned' : 'non-versioned';
        const sourceDesc = testCase.expectedRepairSource ? ` from ${testCase.expectedRepairSource}` : '';
        const testDesc = `${keyDesc} key ${testCase.desc} should give status `
            + `${testCase.expectedRepairStatus}${sourceDesc}`;
        test(testDesc, () => {
            const repairStrategy = getRepairStrategy(
                testCase.followerState,
                testCase.leaderState,
            );
            expect(repairStrategy.status).toEqual(testCase.expectedRepairStatus);
            expect(repairStrategy.source).toEqual(testCase.expectedRepairSource);
            expect(repairStrategy.repairMaster).toEqual(testCase.expectedRepairMaster);
        });
    });
});
